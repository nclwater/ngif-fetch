import requests
from pymongo import MongoClient, DESCENDING
import os
import pandas as pd
import time
from json import JSONDecodeError

mongo_uri = os.getenv('MONGO_URI', 'mongodb://test:password@localhost:27017/test?authSource=admin')

db = MongoClient(mongo_uri).get_database()
readings = db.readings
sensors = db.sensors

usb = [
    'automatic-weather-station/rainfall-accumulation-(tbr2)/raw/historic',
    'automatic-weather-station/rainfall-rate-(tbr2)/raw/historic',
    'automatic-weather-station/rainfall-rate-(tbr1)/raw/historic',
    'automatic-weather-station/rainfall-accumulation-(tbr1)/raw/historic',
    'plant-room-1/rwht-ultrasonic-level-sensor/raw/historic'
]

usb_url = 'https://api.usb.urbanobservatory.ac.uk/api/v2/sensors/timeseries/'

city = [
    'PER_EMOTE_101_SOIL/data/json/?last_n_days=3&data_variable=Soil%20Moisture',
    'PER_EMOTE_102_SOIL/data/json/?last_n_days=3&data_variable=Soil%20Moisture',
    'PER_EMOTE_103_SOIL/data/json/?last_n_days=3&data_variable=Soil%20Moisture',
    'PER_EMOTE_104_SOIL/data/json/?last_n_days=3&data_variable=Soil%20Moisture',
    'PER_EMOTE_105_SOIL/data/json/?last_n_days=3&data_variable=Soil%20Moisture',
]

city_url = 'http://uoweb3.ncl.ac.uk/api/v1.1/sensors/'


def log(s):
    print(f'{pd.Timestamp.now().round("s")} {s}')


def send_data(data, field, name, units):
    last_entry = readings.find_one(
        {'name': name, field: {'$exists': True}}, {'time': 1},
        sort=[('_id', DESCENDING)]
    )

    if last_entry is not None:
        last_time = last_entry['time']
        data = data[data.time > last_time]

    if len(data) > 0:
        log(f'Sending {len(data)} records from {field} ({name})')
        sensors.update_one({'name': name}, {
            '$set': {
                field + '.units': units,
                field + '.last_updated': data.time.iloc[-1],
                field + '.last_value': data[field].iloc[-1]
            }},
                           upsert=True)
        db.readings.insert_many(data.to_dict('records'))


def fetch_usb():

    for sensor in usb:
        try:
            json = requests.get(usb_url + sensor).json()
        except JSONDecodeError:
            continue

        values = json['historic']['values']

        if len(values) == 0:
            continue

        entity = json['timeseries']['parentFeed']['parentEntity']['name']
        units = json['timeseries']['unit']['name']
        field = json['timeseries']['parentFeed']['metric']

        data = pd.DataFrame({'name': entity, 'time': record['time'], field: record['value']} for record in values)
        data['time'] = pd.to_datetime(data.time).dt.tz_localize(None)
        data = data.sort_values('time')

        send_data(data=data, field=field, name=entity, units=units)


def fetch_city():
    for sensor in city:
        try:
            json = requests.get(city_url + sensor).json()
        except JSONDecodeError:
            continue
        values = json['sensors'][0]['data'].get('Soil Moisture')
        if values is None or len(values) == 0:
            continue

        name = 'Urban Sciences Building Green Roof'
        field = json['sensors'][0]['Sensor Name']['0']
        units = values[0]['Units']

        data = pd.DataFrame({'name': name, 'time': record['Timestamp'], field: record['Value']} for record in values)
        data['time'] = pd.to_datetime(data.time, unit='ms')
        data = data.sort_values('time')

        send_data(data=data, field=field, name=name, units=units)


def fetch_envirowatch():
    from xml.etree import ElementTree as ET
    url = "http://api-nclc.envirowatch.ltd.uk/moteservice.asmx"
    headers = {'content-type': 'text/xml'}
    email = os.getenv('ENVIROWATCH_EMAIL')
    password = os.getenv('ENVIROWATCH_PASSWORD')
    assert email is not None, 'Envirowatch email address missing'
    assert password is not None, 'Envirowatch password missing'
    body = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body>
        <Login xmlns="http://envirowatch.ltd.uk/">
          <emailAddress>{email}</emailAddress>
          <password>{password}</password>
        </Login>
      </soap:Body>
    </soap:Envelope>"""

    response = requests.post(url, data=body, headers=headers)
    root = ET.fromstring(response.text)
    token = root.find('.//{http://envirowatch.ltd.uk/}token').text

    body = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body>
        <GetLatest xmlns="http://envirowatch.ltd.uk/">
          <token>{token}</token>
        </GetLatest>
      </soap:Body>
    </soap:Envelope>"""
    response = requests.post(url, data=body, headers=headers)

    root = ET.fromstring(response.text)
    for sensor in [7001, 7002, 7003, 7004, 7005]:
        name = 'Swale'
        field = f'Soil moisture ({sensor})'
        data = pd.DataFrame(
            {
                'name': name,
                'time': pd.to_datetime(elem.find('{http://envirowatch.ltd.uk/}TimeStamp').text),
                field: int(elem.find('{http://envirowatch.ltd.uk/}Reserved').text) / 10,

            } for elem in root.findall(f".//*[{{http://envirowatch.ltd.uk/}}SensorId='{sensor}']"))

        if len(data) == 0:
            continue

        data = data.sort_values('time')

        send_data(data, field, name, units='% VWC')


if __name__ == '__main__':
    interval = 30
    while True:
        start = pd.Timestamp.now()

        try:
            fetch_usb()
        except ConnectionError:
            log('Could not fetch USB data')
        try:
            fetch_city()
        except ConnectionError:
            log('Could not fetch city data')

        fetch_envirowatch()

        end = pd.Timestamp.now()
        duration = (end - start).total_seconds()

        if duration < interval:
            wait = interval - duration
            log(f'Waiting for {int(round(wait))} second(s)')
            time.sleep(wait)
