#!/bin/bash

# This script is used to run the file in the container
rm -r output/*
flink run --python main.py