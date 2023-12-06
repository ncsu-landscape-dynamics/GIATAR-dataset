"""
File: data_update/0a_create_env.py
Author: Ariel Saffer
Date created: 2023-04-14
Description: Create the .env file with workspace-relevant information
"""

# To create the database for the first time, you will also need to download the 
# GBIF taxonomic backbone. 
# Go to https://www.gbif.org/occurrence/download and select the Download tab. 
# Select “Species List” (the last option). 
# You should get an email notification when your download is available. 
# Save file as species lists/by_database/gbif_all_small.csv

# Where the .csv files are being stored (data_dir)

drive_letter = "Y:" 
data_dir = "/GITAR/Database/"

# Auth token for EPPO API

eppo_token = "insert"  # Anyone can register on EPPO (https://data.eppo.int/user/login) to get a token

# Year to start collecting GBIF records

base_obs_year = 1970

# Store information about last updates

gbif_obs_last_update = "2023-11-01"
eppo_report_last_update = "2023-11-01"

with open(".env", "w") as f:
    f.write(f"DATA_PATH='{drive_letter + data_dir}'\n")
    f.write(f"EPPO_TOKEN='{eppo_token}'\n")
    f.write(f"BASE_OBS_YEAR='{base_obs_year}'\n")
    f.write(f"GBIF_OBS_UPDATED='{gbif_obs_last_update}'\n")
    f.write(f"EPPO_REP_UPDATED='{eppo_report_last_update}'\n")
    f.close()