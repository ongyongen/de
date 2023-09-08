"""
This file contains methods used in the Extract phase of the ETL pipeline
"""

import requests
from constants import dataframe, urls

def extract_restaurants_data():
    """
    GET restaurant data from the url link provided

    Input : None
    Output : Object
    """
    url = urls.RESTAURANTS_DATA_URL
    response = requests.get(url, timeout=20)
    return response.json()


def extract_restaurant_records_from_parsed_json(data):
    """
    Extract data for each restaurant from the JSON file and 
    collect them into a single list for ease of further processing

    Input : list
    Output : list
    """

    restaurant_records = []
    for record in data:
        total_records = record['results_shown']
        if total_records > 0:
            restaurant_records += record['restaurants']
    return restaurant_records


def extract_countries_data(countries):
    """
    Create a dictionary to store key-value of pairs of
    { country code : country name } mapping

    Input : dataframe
    Output : dictionary
    """

    d_countries = {}
    for index in range(len(countries)):
        country_name = countries.loc[index, dataframe.COUNTRY]
        country_code = countries.loc[index, dataframe.COUNTRY_CODE]
        d_countries[country_code] = country_name
    return d_countries
