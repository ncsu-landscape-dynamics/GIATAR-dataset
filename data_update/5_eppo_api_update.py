
"""
File: data_update/5_eppo_api_update.py
Author: Ariel Saffer
Date created: 2023-04-14
Description: Query EPPO and update all data for all species
"""

import pandas as pd
import os
import sys
import dotenv
from datetime import date

sys.path.append(os.getcwd())

from data_update.data_functions import eppo_query_wrapper, eppo_cat_api

# Get today's date as date updated
todays_date = date.today()

# Load dotenv file
dotenv.load_dotenv(".env")

# Get invasive database folder data directory
data_dir = os.getenv("DATA_PATH")

# Get EPPO token
token = os.getenv("EPPO_TOKEN")

# Test the EPPO token
if eppo_cat_api("BEMITA", token) == {'message': 'You do not have sufficent rights to perform this operation'}:
    print("Invalid EPPO token. Please update your .env file!")
    sys.exit()

# Define all query options (omitted: general and taxonomy)
# general = ""
# taxonomy = f"/taxonomy"
names = f"/names"
categorization = f"/categorization"
hosts = f"/hosts"

queries = [
    categorization,
    hosts,
    names,
]

# Read in the list of EPPO invasive species
# eppo_species = pd.read_csv(data_dir + "link files/EPPO_link.csv")

# Bring in EPPO invasive records

eppo_link = pd.read_csv(data_dir + "link files/EPPO_link.csv")
eppo_new = pd.read_csv(data_dir + "species lists/new/eppo_new.csv")
eppo_species = eppo_link.loc[eppo_link.usageKey.isin(eppo_new.usageKey)].reset_index(drop=True)

queries = [
    categorization,
    hosts,
    names,
]

for query in queries:
    eppo_query_wrapper(eppo_species, query, token, append=True)