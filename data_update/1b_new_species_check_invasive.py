"""
File: data_update/1b_new_species_check_invasive.py
Author: Ariel Saffer
Date created: 2023-04-14
Description: Check the invasive status of species from all lists
"""

import pandas as pd
import numpy as np
import os
import sys
import dotenv

from datetime import date

sys.path.append(os.getcwd())

from data_update.data_functions import eppo_cat_api

dotenv.load_dotenv(".env")
data_dir = os.getenv("DATA_PATH")
token = os.getenv("EPPO_TOKEN")

# Test the EPPO token
if eppo_cat_api("BEMITA", token) == {'message': 'You do not have sufficent rights to perform this operation'}:
    print("Invalid EPPO token. Please update your .env file!")
    sys.exit()

# Get today's date
today = date.today()

# Bring back in the new datasets

cabi_new = pd.read_csv(data_dir + "species lists/gbif_matched/cabi_gbif.csv")
cabi_new = cabi_new.loc[cabi_new["New"]==True]

eppo_new = pd.read_csv(data_dir + "species lists/gbif_matched/eppo_gbif.csv")
eppo_new = eppo_new.loc[eppo_new["New"]==True]

asfr_new = pd.read_csv(data_dir + "species lists/gbif_matched/asfr_gbif.csv")
asfr_new = asfr_new.loc[asfr_new["New"]==True]

# All ASFR assumed invasive

asfr_new.to_csv(data_dir + "species lists/new/asfr_new.csv", index=False)

# EPPO - has categorization

print(f"Getting EPPO categorization for {len(eppo_new)} species")
eppo_new["categorization"] = eppo_new.codeEPPO.apply(lambda x: eppo_cat_api(x, token))
eppo_new["invasive"] = np.where(eppo_new["categorization"].map(len) > 0, True, False)

# Append and write to csv

eppo_gbif_inv = pd.read_csv(
    data_dir + "species lists/gbif_matched/eppo_gbif_with_categ.csv")
eppo_gbif_inv["New"] = False

eppo_gbif_inv = pd.concat([eppo_gbif_inv, eppo_new], ignore_index=True)
# Since these are all new from GBIF, should be mutually exlcusive

# Note: we may want to periodically check invasive == False values to see if the species have since become invasive

eppo_gbif_inv.to_csv(
    data_dir + "species lists/gbif_matched/eppo_gbif_with_categ.csv", index=False
)

eppo_new.to_csv(data_dir + "species lists/new/eppo_new.csv", index=False)

# CABI check will happen elsewhere

cabi_new.to_csv(data_dir + "species lists/new/cabi_new.csv", index=False)

# Export a list (as .csv) of all new species usageKeys (to run the full EPPO and CABI queries on)

new_species = pd.concat([cabi_new, eppo_new, asfr_new], ignore_index=True)
new_species.loc[new_species["usageKey"].notna()]["usageKey"].to_csv(
    data_dir + "species lists/new/new_usageKeys.csv", index=False
)

print(
    f"Exported {len(new_species.loc[new_species['usageKey'].notna()])} new usageKeys, including"
    f" {len(new_species.loc[new_species['invasive']==True])} invasive species.")


# Next:
# CABI to check invasive and get data
