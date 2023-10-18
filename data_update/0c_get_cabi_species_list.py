"""
File: data_updated/0c_get_cabi_species_list.py
Author: Ariel Saffer
Date created: 2023-04-14
Description: Reformat the CABI Invasive Species Compendium species list
"""

import pandas as pd

import os
import dotenv

from datetime import date

dotenv.load_dotenv(".env")
data_dir = os.getenv("DATA_PATH")

# Get today's date
today = date.today()

# Get new lists: currently manual, could be coded (selenium, login required)

### Get current CABI list

### Download ISC Species list

# From: https://www.cabidigitallibrary.org/journal/cabicompendium/isdt#
# Select and unselect a filter option to display full list
# Download as CSV and save to species lists/by_database/ISCSearchResults.csv
# Remove any headers and make sure columns are called "Scientific name", "Common name", "Coverage", "URL"
# (We didn't code this in because they change it all the time...)

cabi = pd.read_csv(data_dir + "species lists/by_database/ISCSearchResults.csv")

# Keep only columns: Scientific name	Common name	Coverage	URL

cabi_list = cabi[["Scientific name","Common name","Coverage","URL"]]

cabi_list['New'] = True
cabi_list['Date'] = f"{today.year}-{today.month:02d}-{today.day:02d}"

# Compare

prev_cabi_list = pd.read_csv(data_dir + "species lists/by_database/cabi_full_list.csv")
prev_cabi_list['New'] = False

cabi_list = pd.concat([prev_cabi_list,cabi_list]).drop_duplicates(subset=['URL'], keep="first")

print("How many new (TRUE) and old (FALSE) records?")
print(cabi_list['New'].value_counts())

# Export consolidated list

cabi_list.to_csv(data_dir + "species lists/by_database/cabi_full_list.csv", index=False)
