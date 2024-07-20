"""
File: data_update/0b2_get_sinas_species_list.py
Author: Ariel Saffer
Date created: 2023-04-14
Description: Reformat the SInAS data downloaded from Zenodo
"""

import pandas as pd

import os
import dotenv

from datetime import date

dotenv.load_dotenv(".env")
data_dir = os.getenv("DATA_PATH")

# Get today's date
today = date.today()

# Download the latest SInAS list and records from https://zenodo.org/records/10038256 (if available)
# and save as:  species lists/by_database/SInAS_AlienSpeciesDB_2.5_FullTaxaList.csv
# and: species lists/by_database/SInAS_AlienSpeciesDB_2.5.csv

sinas_list = pd.read_csv(data_dir + "species lists/by_database/SInAS_AlienSpeciesDB_2.5_FullTaxaList.csv", sep=" ")

sinas_list['New'] = True
sinas_list["Date"] = f"{today.year}-{today.month:02d}-{today.day:02d}"


# Compare
try:
    prev_sinas_list = pd.read_csv(data_dir + "species lists/by_database/sinas_full_list.csv")
    prev_sinas_list['New'] = False
    sinas_list = pd.concat([prev_sinas_list,sinas_list]).drop_duplicates(subset=['Taxon'], keep="first")
except FileNotFoundError:
    sinas_list = sinas_list.drop_duplicates(subset=['Taxon'], keep="last")

print("How many new (TRUE) and old (FALSE) records?")
print(sinas_list['New'].value_counts())

# Set usageKey as string and remove the float decimal

sinas_list['GBIFusageKey'] = sinas_list['GBIFusageKey'].astype(str)
sinas_list['GBIFusageKey'] = sinas_list['GBIFusageKey'].str.replace("\.0", "", regex=True)

# Export consolidated list

sinas_list.to_csv(data_dir + "species lists/by_database/sinas_full_list.csv", index=False)