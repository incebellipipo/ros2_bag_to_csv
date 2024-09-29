#!/usr/bin/env python3

import sys, os

sys.path.append(os.path.join("."))

import numpy as np
import pandas as pd

from ..src.ros2_bag_to_pandas import get_data as putils

def join_csv_files(directory, output_file, timestamp_column='timestamp'):
    # Initialize an empty list to hold dataframes
    dataframes = []

    print(os.listdir(directory))

    # Iterate over all CSV files in the directory
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            if not 'rosbag2' in filename:
                continue

            file_path = os.path.join(directory, filename)

            # Read each CSV, ensuring the timestamp column is set as index
            df = pd.read_csv(file_path, index_col=timestamp_column, parse_dates=[timestamp_column])

            # Append the dataframe to the list
            dataframes.append(df)

    # Concatenate all dataframes along the rows (axis=0)
    concatenated_df = pd.concat(dataframes, axis=0)

    # Sort by the index to ensure time order is maintained
    concatenated_df = concatenated_df.sort_index()

    # Save the concatenated dataframe to a new CSV file
    concatenated_df.to_csv(directory + '/' + output_file)

    print(f"All CSV files have been concatenated and saved to {output_file}")

def main():
    bag_files = putils.find_rosbag_files(from_dir='../data')


    for bag_path in bag_files:
        # Usage Example
        directory = os.path.dirname(bag_path) + "/.."
        output_file = 'data.csv'
        join_csv_files(directory, output_file)



if __name__ == "__main__":
    main()

