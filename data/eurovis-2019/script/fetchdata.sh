#!/bin/sh
set -e

# Data taken from the VDL account

# Fetch raw CSV file.
curl -O https://raw.githubusercontent.com/visdesignlab/mvnv-study/master/data/s_network_large_directed_multiEdge.json

# Process the raw file into Multinet CSVs.
python process.py < s_network_large_directed_multiEdge.json
