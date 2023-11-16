import requests
import time
from pandas import DataFrame
from decouple import config
from helper_functions import json_zip_writer

from dagster_aws.s3 import S3Coordinate
# Constants
COUNTRIES_URL = config('COUNTRIES_URL')
API_KEY = config('API_KEY')
API_HOST = config('API_HOST')

headers = {
    "X-RapidAPI-Key": API_KEY,
    "X-RapidAPI-Host": API_HOST
}

# Function to make an API request and handle responses
def make_api_request(url, params=None):
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()['data']
    else:
        return None

# Function to get a list of countries
def get_countries():
    url = COUNTRIES_URL
    offset = 0
    limit = 10
    data = []

    for _ in range(1):  # Perform 1 iteration (adjust as needed)
        params = {"limit": limit, "offset": offset}
        countries_data = make_api_request(url, params)

        if countries_data:
            data.extend([(country['name'], country['code']) for country in countries_data])
            offset += limit
        else:
            print("Failed to retrieve data.")
        time.sleep(1)

    countries = DataFrame(data, columns=["Name", "Code"])

    json_zip_writer(countries.to_json(orient='records'), "data/countries.json.gz")

    return countries

# Function to get details for each country
def get_country_details(country):
    country_details = []

    for index, row in country.iterrows():
        country_code = row['Code']
        country_url = f"https://wft-geo-db.p.rapidapi.com/v1/geo/countries/{country_code}"
        response_data = make_api_request(country_url)

        if response_data:
            country_details.append(response_data)
        else:
            print(f"Failed to retrieve details for {row['Name']} ({country_code}). Status code:", response_data.status_code)
        time.sleep(1)

        country_detail = DataFrame(country_details)

        json_zip_writer(country_detail.to_json(orient='records'), "data/country_details.json.gz")

    return country_detail

# Function to get place details for each country
def get_country_places(country):

    for index, row in country.iterrows():
        country_code = row['Code']
        country_url = f"https://wft-geo-db.p.rapidapi.com/v1/geo/countries/{country_code}/places"
        response_data = make_api_request(country_url)

        if response_data:
            places = DataFrame(response_data)
        else:
            print(f"Failed to retrieve place details for {row['Name']} ({country_code}). Status code:", response_data.status_code)
        time.sleep(1)


    json_zip_writer(places.to_json(orient='records'), "data/country_places.json.gz")
        
    return places

