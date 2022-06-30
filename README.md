# NGIF Fetch

![build](https://github.com/nclwater/ngif-fetch/workflows/build/badge.svg)

Python script that periodically pulls data from external APIs and sends it to the NGIF database

## Usage
`docker run -d --env MONGO_URI=mongodb://user:password@hostname:27017/database?authSource=admin --env ENVIROWATCH_EMAIL=user.name@domain.com --env ENVIROWATCH_PASSWORD=password --restart always --name ngif-fetch fmcclean/ngif-fetch`

## Dependencies
`pip install -r requirements.txt`

## Tests
`python -m unittest`
