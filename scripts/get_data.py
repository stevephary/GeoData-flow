import requests
import time
import pandas as pd
from decouple import config


COUNTRIES_URL = config('COUNTRIES_URL')
API_KEY = config('API_KEY')
API_HOST = config('API_HOST')

headers = {
        "X-RapidAPI-Key": API_KEY,
        "X-RapidAPI-Host": API_HOST
    }


def get_countries():
    url = COUNTRIES_URL

    # Set the initial offset and limit
    offset = 0
    limit = 10

    # Create an empty list to store the data
    data = []

    for iteration in range(1):  # Perform 10 iterations
        params = {"limit": limit, "offset": offset}

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            countries_data = response.json()
            for country in countries_data['data']:
                data.append([country['name'], country['code']])

            # Increment the offset for the next iteration
            offset += limit

        else:
            print("Failed to retrieve data. Status code:", response.status_code)

        time.sleep(1)

    # Create a DataFrame for countries
    countries = pd.DataFrame(data, columns=["Name", "Code"])
    
    return countries

def get_country_details(countries_df):

    country_details = []

    for index, row in countries_df.iterrows():
        country_code = row['Code']
        country_url = f"https://wft-geo-db.p.rapidapi.com/v1/geo/countries/{country_code}"
        response = requests.get(country_url, headers=headers)
        
        if response.status_code == 200:
            country_data = response.json()
            country_details.append(country_data)
        else:
            print(f"Failed to retrieve details for {row['Name']} ({country_code}). Status code:", response.status_code)
        time.sleep(1)
    
    return country_details

def get_country_places(places):
    place_details = []
    
    for index, row in places.iterrows():
        country_code = row['Code']
        country_url = f"https://wft-geo-db.p.rapidapi.com/v1/geo/countries/{country_code}/places"
        response = requests.get(country_url, headers=headers)
        
        if response.status_code == 200:
            place_data = response.json()
            place_details.append(place_data)
        else:
            print(f"Failed to retrieve place details for {row['Name']} ({country_code}). Status code:", response.status_code)
        time.sleep(1)
    
    return place_details