#!/bin/sh

set -e

sudo apt update

sudo apt install -y \
    ros-humble-rosbag2-py \
    ros-humble-rosidl-runtime-py \
    ros-humble-rclpy \
    python3-matplotlib \
    python3-numpy \
    python3-pandas \
    python3-pip \
    python3-ipywidgets \
    vim

echo "source /opt/ros/humble/setup.bash" >> ~/.bashrc