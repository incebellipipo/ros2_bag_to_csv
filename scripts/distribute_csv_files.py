#!/usr/bin/env python3

import sys, os

sys.path.append(os.path.join("."))

import numpy as np
import pandas as pd

from ..src.ros2_bag_to_pandas import get_data as putils

import matplotlib.pyplot as plt


def main():
    bag_files = putils.find_rosbag_files()

    conversion_keywords = [
        {
            "topic": "<topic_name>",
            "output_file": "<file_name>.csv",
            "prefix": "<prefix>",
        }
    ]

    for conversion in conversion_keywords:
        for bag_path in bag_files:
            try:
                print(f"Processing {bag_path}")
                topic_name = conversion["topic"]

                bag_dir = os.path.dirname(bag_path)

                output_file = os.path.join(
                    bag_dir,
                    f"../{os.path.basename(bag_path).replace('.db3', '')}{conversion['output_file']}",
                )
                msgs = putils.read_msg_from_bag_as_dataframe(bag_path, topic_name)

                unpacked_df = msgs["message"].apply(
                    lambda x: putils.unpack_message(x, prefix=conversion["prefix"])
                )

                # Concatenate the unpacked data with the original DataFrame
                final_dataframe = pd.concat(
                    [msgs.drop(columns=["message"]), unpacked_df], axis=1
                )

                final_dataframe.to_csv(output_file)

                print(f"Plot saved as {output_file}")
            except Exception as e:
                print(f"Failed to process {bag_path}: {e}")


if __name__ == "__main__":
    main()
