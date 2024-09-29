#!/usr/bin/env python3

import os
import pandas as pd
import numpy as np

import rosbag2_py
from rosidl_runtime_py.utilities import get_message
from rclpy.serialization import deserialize_message


def read_msg_data_from_bag(bag_path, topic_name):
    """
    Read messages from a ROS2 bag file and return them as a list.

    This function reads messages from a ROS2 bag file and returns them as a list.
    The messages are filtered by the specified topic name. The messages are
    returned as a list of message objects. The message objects can be unpacked
    into separate columns using the `unpack_message` function. This method does
    not return publish time information.


    Parameters
    ----------
    bag_path : str
        The path to the ROS2 bag file.
    topic_name : str
        The name of the topic to read messages from.

    Returns
    -------
    list
        A list containing the messages from the specified topic.
    """

    storage_options = rosbag2_py.StorageOptions(
        uri=bag_path, storage_id='sqlite3')
    converter_options = rosbag2_py.ConverterOptions('', '')
    reader = rosbag2_py.SequentialReader()
    try:
        reader.open(storage_options, converter_options)
    except Exception as e:
        print(f"Failed to open bag file: {e}")
        return []

    topic_types = reader.get_all_topics_and_types()
    type_map = {topic.name: topic.type for topic in topic_types}

    msgs = []

    while reader.has_next():
        (topic, data, t) = reader.read_next()
        if topic == topic_name:
            msg_type = get_message(type_map[topic])
            msg = deserialize_message(data, msg_type)
            msgs.append(msg)

    return msgs


def read_msg_from_bag_as_dataframe(bag_path, topic_name):
    """
    Read messages from a ROS2 bag file and return them as a pandas DataFrame.

    The data is returned as a DataFrame with two columns: 'timestamp' and
    'message'. The 'timestamp' column contains the message timestamp in
    nanoseconds that represents the moment when the message was published, and
    the 'message' column contains the message object. The'message' column can be
    unpacked into separate columns using the `unpack_message` function.

    Parameters
    ----------
    bag_path : str
        The path to the ROS2 bag file.
    topic_name : str
        The name of the topic to read messages from.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the messages from the specified topic.
    """

    storage_options = rosbag2_py.StorageOptions(
        uri=bag_path, storage_id='sqlite3')
    converter_options = rosbag2_py.ConverterOptions('', '')
    reader = rosbag2_py.SequentialReader()
    try:
        reader.open(storage_options, converter_options)
    except Exception as e:
        print(f"Failed to open bag file: {e}")
        return []

    topic_types = reader.get_all_topics_and_types()
    type_map = {topic.name: topic.type for topic in topic_types}

    timestamps = []
    msgs = []

    while reader.has_next():
        # Read the next message from the bag file
        # The message is a tuple containing the topic name, message data, and
        # timestamp. Timestamp represents the moment when the message was
        # published.
        (topic, data, t) = reader.read_next()
        if topic == topic_name:
            msg_type = get_message(type_map[topic])
            msg = deserialize_message(data, msg_type)
            timestamps.append(t)
            msgs.append(msg)

    df = pd.DataFrame({
        'timestamp': timestamps,
        'message': msgs
    })

    df.set_index('timestamp', inplace=True)
    # df.index = pd.to_datetime(df.index, unit='ns')

    return df

def read_unpackaged_message_from_bag_as_dataframe(bag_path, topic_name, conversion):
    """
    Read messages from a ROS2 bag file and return them as a pandas DataFrame.

    The data is returned as a DataFrame with two columns: 'timestamp' and
    'message'. The 'timestamp' column contains the message timestamp in
    nanoseconds that represents the moment when the message was published, and
    the 'message' column contains the message object. The'message' column can be
    unpacked into separate columns using the `unpack_message` function.

    Parameters
    ----------
    bag_path : str
        The path to the ROS2 bag file.
    topic_name : str
        The name of the topic to read messages from.
    conversion : dict
        A dictionary containing the conversion information for the topic.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the messages from the specified topic.
    """
    msgs = read_msg_from_bag_as_dataframe(bag_path, topic_name)

    # unpacked_df = msgs["message"].apply(
    #     lambda x: unpack_message(x, prefix=conversion["prefix"])
    # )

    # # Concatenate the unpacked data with the original DataFrame
    # final_dataframe = pd.concat(
    #     [msgs.drop(columns=["message"]), unpacked_df], axis=1
    # )

    unpacked_columns = msgs["message"].apply(
        lambda x: unpack_message(x, prefix=conversion["prefix"])
    )

    # Drop the "message" column and join the unpacked columns directly
    msgs = msgs.drop(columns=["message"]).join(unpacked_columns)

    return msgs


def find_rosbag_files(from_dir='.'):
    """
    Find all ROS2 bag files in the specified directory and its subdirectories.

    This function searches the specified directory and its subdirectories for
    ROS2 bag files. The function returns a list of paths to the bag files. The
    function skips directories that contain a file named 'DATAIGNORE'.

    Parameters
    ----------
    from_dir : str
        The directory to search for ROS2 bag files.

    Returns
    -------
    list
        A list containing the paths to the ROS2 bag files.
    """
    bag_files = []
    for root, dirs, files in os.walk(from_dir):
        for file in files:
            if not file.endswith('.db3'):
                continue

            # check if the directory contains DATAIGNORE file
            data_ignore_file = os.path.join(root, '../', 'DATAIGNORE')
            if os.path.exists(data_ignore_file):
                print(f"Skipping {root} because it contains DATAIGNORE file")
                continue

            bag_files.append(os.path.join(root, file))
    return bag_files


def unpack_message(msg, prefix=""):
    """
    Unpack a ROS2 message object into a pandas Series.

    This function unpacks a ROS2 message object into a pandas Series. The
    function recursively unpacks nested messages and lists of messages. The
    function returns a Series with the message slots as columns. The column
    names are prefixed with the specified prefix. The function returns an empty
    Series if the message type is unknown.

    Parameters
    ----------
    msg : object
        The ROS2 message object to unpack.
    prefix : str
        The prefix to add to the column names.

    Returns
    -------
    pandas.Series
        A Series containing the unpacked message slots.
    """
    if hasattr(msg, "__slots__"):
        # Create a dictionary to hold slot values
        unpacked_data = {}
        for slot in msg.__slots__:
            value = getattr(msg, slot)
            # Recursively unpack if the value is a message with slots
            if hasattr(value, "__slots__"):
                # Unpack nested message
                nested_data = unpack_message(value)
                for nested_key in nested_data.index:
                    unpacked_data[f"{prefix}{slot}{nested_key}"] = nested_data[
                        nested_key
                    ]  # Prefix with the slot name
            elif isinstance(value, list):  # Handle lists of messages
                for idx, item in enumerate(value):
                    if hasattr(item, "__slots__"):  # Unpack items in the list if they are objects
                        nested_data = unpack_message(
                            item, prefix=f"{prefix}{slot}[{idx}]")
                        unpacked_data.update(nested_data)
                    else:
                        # Otherwise, just store the item in its own column
                        unpacked_data[f"{prefix}{slot}[{idx}]"] = item
            else:
                # Otherwise, just assign the value
                unpacked_data[prefix+slot] = value
        return pd.Series(unpacked_data)
    return pd.Series(dtype="object")  # Return empty Series for unknown types
