"""
This file contains methods used in the Load phase of the ETL pipeline
"""

def export_dataframe_to_csv(dataframe, csv_file_path):
    """
    Convert a dataframe into a CSV file for export

    Input : dataframe, string
    Output : None
    """
    dataframe.to_csv(csv_file_path)
