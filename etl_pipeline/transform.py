"""
This file contains methods used in the Transform phase of the ETL pipeline
"""

from datetime import datetime
from constants import dataframe

from etl_pipeline.helpers import (
    create_template_restaurants_df,
    create_template_events_df,
    map_country_code_to_country_name,
    event_occurs_within_dates,
    replace_na_cells
)

def process_restaurants(d_countries, restaurant_records):
    """
    Collect restaurant record data into the template dataframe for
    cleaned restaurants data

    Input : dictionary, list
    Output : dataframe 
    """

    data_frame = create_template_restaurants_df()

    for index, record in enumerate(restaurant_records):
        restaurant = record["restaurant"]
        data_frame.loc[index, dataframe.RESTAURANT_ID] = restaurant["R"]["res_id"]
        data_frame.loc[index, dataframe.RESTAURANT_NAME] = restaurant["name"]
        data_frame.loc[index, dataframe.COUNTRY_ID] = restaurant["location"]["country_id"]
        data_frame.loc[index, dataframe.CITY] = restaurant["location"]["city"]
        data_frame.loc[index, dataframe.USER_RATING_VOTES] = restaurant["user_rating"]["votes"]
        data_frame.loc[index, dataframe.USER_AGGREGATE_RATING] = \
            restaurant["user_rating"]["aggregate_rating"]
        data_frame.loc[index, dataframe.CUISINES] = restaurant["cuisines"]
        data_frame.loc[index, dataframe.RATING_TEXT] = restaurant["user_rating"]["rating_text"]

        # Use the d_countries dictionary to map country code to country name
        data_frame.loc[index, dataframe.COUNTRY] = map_country_code_to_country_name(
            d_countries, restaurant["location"]["country_id"])

        # If current reecord has an events list, add it to dataframe's "events" column
        # If not, add an empty list to dataframe's "events" column
        if "zomato_events" in record["restaurant"]:
            events = restaurant["zomato_events"]
            data_frame.at[index, dataframe.EVENTS] = events
        else:
            data_frame.at[index, dataframe.EVENTS] = dataframe.EMPTY_EVENTS_CELL

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

def process_restaurant_events_within_date_range(df1, fixed_start, fixed_end):
    """
    Obtain all event data for each restaurant in Apr 2019 (for Q2) based on
    a dataframe of restaurant data

    Input : dataframe, string, string,
    Output : dataframe
    """
    df2 = create_template_events_df()
    index = 0

    # Iterate through each restaurant record and check if it has events
    for i in df1.index:

        events = df1.loc[i, dataframe.EVENTS]
        event_id = df1.loc[i, dataframe.EVENT_ID]
        restaurant_id = df1.loc[i, dataframe.RESTAURANT_ID]
        restaurant_name = df1.loc[i, dataframe.RESTAURANT_NAME]

        if events != dataframe.EMPTY_EVENTS_CELL:
            for event in events:
                event_title = event['event'] ['title']
                start_date = event['event'] ['start_date']
                end_date = event['event'] ['end_date']
                # Check that restaurant's events occured in the stated time period
                # (ie Apr 2019 in this case)
                if event_occurs_within_dates(
                    datetime.strptime(start_date, '%Y-%m-%d'),
                    datetime.strptime(end_date, '%Y-%m-%d'),
                    fixed_start,
                    fixed_end
                ):
                    # For each event, iterate through the associated list of photos
                    # to obtain all event photos' url links.
                    # If the event has more than one photo, the other photo's url links
                    # are concatenated together with a comma separator
                    photo_urls_string = extract_photo_urls(event['event'])

                    # Insert the valid event data (within stated time period)
                    # into the new dataframe for Q2
                    df2.loc[index, dataframe.EVENT_ID] =  event_id
                    df2.loc[index, dataframe.RESTAURANT_ID] = restaurant_id
                    df2.loc[index, dataframe.RESTAURANT_NAME] = restaurant_name
                    df2.loc[index, dataframe.PHOTO_URL] = photo_urls_string
                    df2.loc[index, dataframe.EVENT_TITLE] = event_title
                    df2.loc[index, dataframe.EVENT_START_DATE] = start_date
                    df2.loc[index, dataframe.EVENT_END_DATE] = end_date
                    index += 1

    return df2


def prepare_data_for_q1(data_frame):
    """
    Do final processing for template dataframe and parse it to 
    fit the requirements of q1 (ie only include columns required by q1)
        - Replace all NaN cells with NA (if any)
        - Only include columns required in q1

    Input : dataframe
    Output : dataframe 
    """
    data_frame = replace_na_cells(data_frame, dataframe.NA_VALUE)
    return data_frame[[
        dataframe.RESTAURANT_ID,
        dataframe.RESTAURANT_NAME,
        dataframe.COUNTRY,
        dataframe.CITY,
        dataframe.USER_RATING_VOTES,
        dataframe.USER_AGGREGATE_RATING,
        dataframe.CUISINES
    ]]


def prepare_data_for_q2(data_frame):
    """
    Do final processing for template dataframe and parse it to 
    fit the requirements of q2 
        - Replace all NaN cells with NA (if any)
        - Reset column indexes to be in fixed intervals ie 1,2,3...
        - Only include columns required in q2

    Input : dataframe
    Output : dataframe 
    """

    data_frame = replace_na_cells(data_frame, dataframe.NA_VALUE)

    data_frame = data_frame.reset_index()
    data_frame = data_frame.drop(columns=["index"])

    data_frame = data_frame[[
        dataframe.EVENT_ID,
        dataframe.RESTAURANT_ID,
        dataframe.RESTAURANT_NAME,
        dataframe.PHOTO_URL,
        dataframe.EVENT_TITLE,
        dataframe.EVENT_START_DATE,
        dataframe.EVENT_END_DATE,
    ]]

    return data_frame
