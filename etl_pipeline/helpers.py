"""
This file contains helper methods used throughout the phases of the ETL pipeline
"""

from datetime import datetime
import pandas as pd
from constants import dataframe

def create_df(columns):
    """
    Create a template dataframe based on columns provided

    Input : dictioary - { column name : column data type }
    Output : dataframe 
    """
    data_frame = pd.DataFrame(columns)
    return data_frame

def create_template_restaurants_df():
    """
    Create a template dataframe for the cleaned restaurants data
    (used for Q1 CSV output)

    Input : None
    Output : dataframe 
    """
    columns = {
        dataframe.RESTAURANT_ID: pd.Series(dtype='int'),
        dataframe.RESTAURANT_NAME: pd.Series(dtype='str'),
        dataframe.COUNTRY: pd.Series(dtype='str'),
        dataframe.CITY: pd.Series(dtype='str'),
        dataframe.USER_RATING_VOTES: pd.Series(dtype='str'),
        dataframe.USER_AGGREGATE_RATING: pd.Series(dtype='str'),
        dataframe.CUISINES: pd.Series(dtype='str'),
        dataframe.COUNTRY_ID: pd.Series(dtype='int'),
        dataframe.RATING_TEXT: pd.Series(dtype='str'),
        dataframe.PHOTO_URL: pd.Series(dtype='str'),
        dataframe.EVENTS: pd.Series(dtype='object'),
        dataframe.EVENT_ID: pd.Series(dtype='str'),
        dataframe.EVENT_TITLE: pd.Series(dtype='str'),
        dataframe.EVENT_START_DATE: pd.Series(dtype='str'),
        dataframe.EVENT_END_DATE: pd.Series(dtype='str')
    }

    data_frame = create_df(columns)
    return data_frame

def create_template_events_df():
    """
    Create a template dataframe for the cleaned restaurant events data
    in Apr 2019 (used for Q2 CSV output)

    Input : None
    Output : dataframe 
    """
    columns = {
        dataframe.EVENT_ID: pd.Series(dtype='str'),
        dataframe.RESTAURANT_ID: pd.Series(dtype='int'),
        dataframe.RESTAURANT_NAME: pd.Series(dtype='str'),
        dataframe.PHOTO_URL: pd.Series(dtype='str'),
        dataframe.EVENT_TITLE: pd.Series(dtype='str'),
        dataframe.EVENT_START_DATE: pd.Series(dtype='str'),
        dataframe.EVENT_END_DATE: pd.Series(dtype='str'),
    }

    data_frame = create_df(columns)
    return data_frame

def map_country_code_to_country_name(d_countries, country_code):
    """
    Returns the country name associated with the provided country code
    based on the countries dictionary of { country code : country name }
    If country code does not exist, return NA

    Input : dictionary, string
    Output : string 
    """
    if country_code in d_countries:
        return d_countries[country_code]
    return dataframe.NA_VALUE

def event_occurs_within_dates(
    event_start,
    event_end,
    fixed_start,
    fixed_end
):
    """
    Check whether an event occurs within a date range 

    Input : string, string, string, string
    Output : boolean 
    """
    return (
        event_start >= datetime.strptime(fixed_start, '%Y-%m-%d')
        and event_end <= datetime.strptime(fixed_end, '%Y-%m-%d')
    )


def replace_na_cells(data_frame, replacement_str):
    """
    Replace NaN cells in the dataframe with a provided
    replacement string

    Input : dataframe, string 
    Output : dataframe 
    """
    data_frame = data_frame.fillna(replacement_str)
    return data_frame

def extract_photo_urls(event):
    """
    Obtain photo URLs for all photos of each event.
    If there's multiple photo URLs, they are separated by
    a comma delimiter

    Input : list
    Output : string
    """
    if 'photos' in event:
        photo_urls = list(map(lambda photo: photo['photo']['url'], event['photos']))
        photo_urls_string = ",".join(photo_urls)
        return photo_urls_string
    return dataframe.NA_VALUE