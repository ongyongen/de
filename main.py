"""
This file contains the main script to run 
so as to initiate the data cleaning process
"""

from datetime import datetime
import pandas as pd
from etl_pipeline import (
    extract_countries_data,
    extract_restaurants_data,
    extract_restaurant_records_from_parsed_json,
    process_restaurants,
    process_restaurant_events_within_date_range,
    prepare_data_for_q1,
    prepare_data_for_q2,
    export_dataframe_to_csv
)

if __name__ == '__main__':
    print("=======================================================")
    print("Data processing script for restaurants data from Zomato")
    print("=======================================================")

    data = extract_restaurants_data()
    print("- Restaurants data is read")

    countries = pd.read_excel('./data_files/Country-Code.xlsx')
    print("- Countries data is read")

    d_countries = extract_countries_data(countries)
    restaurant_records = extract_restaurant_records_from_parsed_json(data)

    df1 = process_restaurants(d_countries, restaurant_records)
    df2 = process_restaurant_events_within_date_range(df1, '2019-04-01', '2019-04-30')

    print("-The dataframes for Q1 and Q2 are processed")
    q1_df = prepare_data_for_q1(df1)
    q2_df = prepare_data_for_q2(df2)

    curr_time = datetime.now()
    q1_filename = f"sample_output/q1_{curr_time}.csv"
    q2_filename = f"sample_output/q2_{curr_time}.csv"

    export_dataframe_to_csv(df1, q1_filename)
    export_dataframe_to_csv(df1, q2_filename)
    print("- The dataframes for Q1 and Q2 are exported to sample_output folder")


    print("\n")
    print("=========================")
    print("Preview of Q1's dataframe")
    print("=========================")
    print(q1_df.head(5))
    print("\n")

    print("=========================")
    print("Preview of Q2's dataframe")
    print("=========================")
    print(q2_df.head(5))
    print("\n")

    print("=========================")
    print("Q3 Analysis")
    print("=========================")
    print("\n")
    