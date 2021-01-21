FROM python:3.8.6-slim

RUN mkdir /src

WORKDIR /src

COPY requirements.txt ./

RUN pip install -r requirements.txt

COPY fetch.py ./

CMD python -u fetch.py
