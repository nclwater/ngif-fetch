import requests
from pymongo import MongoClient, DESCENDING
import os
import pandas as pd
import time

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


def send_data(data, field, name, units):
    last_entry = readings.find_one(
        {field: {'$exists': True}}, {'time': 1},
        sort=[('_id', DESCENDING)]
    )

    if last_entry is not None:
        last_time = last_entry['time']
        data = data[data.time > last_time]

    if len(data) > 0:
        print(f'{pd.Timestamp.now().round("s")} Sending {len(data)} records from {field} ({name})')
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
        json = requests.get(usb_url + sensor).json()

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
        json = requests.get(city_url + sensor).json()
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


if __name__ == '__main__':
    while True:
        fetch_usb()
        fetch_city()
        time.sleep(15 * 60)
