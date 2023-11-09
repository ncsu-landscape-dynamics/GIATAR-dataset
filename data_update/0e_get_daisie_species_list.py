"""
File: data_update/0b_get_asfr_species_list.py
Author: Ariel Saffer
Date created: 2023-11-01
Description: Reformat the DAISIE species list data downloaded from Github
"""

import pandas as pd

import os
import dotenv

from datetime import date

dotenv.load_dotenv(".env")
data_dir = os.getenv("DATA_PATH")

# Get today's date
today = date.today()

# Download the latest version of input_taxon.csv (save as DAISIE_taxon.csv) 
# from https://github.com/trias-project/daisie-checklist/tree/master/data/raw (if available)
# If not, skip this script!

daisie_list = pd.read_csv(data_dir + "species lists/by_database/DAISIE_taxon.csv")

daisie_list['New'] = True
daisie_list["Date"] = f"{today.year}-{today.month:02d}-{today.day:02d}"

# Create the scientific name column by combining genus and species

daisie_list['scientificName'] = daisie_list['genus'] + " " + daisie_list['species']

# Compare

prev_daisie_list = pd.read_csv(data_dir + "species lists/by_database/daisie_full_list.csv")
prev_daisie_list['New'] = False

daisie_list = pd.concat([prev_daisie_list,daisie_list]).drop_duplicates(subset=['scientificName'], keep="first")

print("How many new (TRUE) and old (FALSE) records?")
print(daisie_list['New'].value_counts())

# Export consolidated list

daisie_list.to_csv(data_dir + "species lists/by_database/daisie_full_list.csv", index=False)
