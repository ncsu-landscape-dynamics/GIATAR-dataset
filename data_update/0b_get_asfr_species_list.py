"""
File: data_update/0b_get_asfr_species_list.py
Author: Ariel Saffer
Date created: 2023-04-14
Description: Reformat the Alien Species First Record data downloaded from Zenodo
"""

import pandas as pd

import os
import dotenv

from datetime import date

dotenv.load_dotenv(".env")
data_dir = os.getenv("DATA_PATH")

# Get today's date
today = date.today()

# Download the latest ASFR list from https://zenodo.org/record/3690742#.ZDbV4nbMIQ8 (if available)
# If not, skip this script!

asfr_list = pd.read_csv(data_dir + "species lists/by_database/AlienSpeciesFirstRecord.csv")

asfr_list['New'] = True
asfr_list["Date"] = f"{today.year}-{today.month:02d}-{today.day:02d}"


# Compare
try:
    prev_asfr_list = pd.read_csv(data_dir + "species lists/by_database/asfr_full_list.csv")
    prev_asfr_list['New'] = False

    asfr_list = pd.concat([prev_asfr_list,asfr_list]).drop_duplicates(subset=['scientificName'], keep="first")
except FileNotFoundError:
    pass

print("How many new (TRUE) and old (FALSE) records?")
print(asfr_list['New'].value_counts())

# Export consolidated list

asfr_list.to_csv(data_dir + "species lists/by_database/asfr_full_list.csv", index=False)
