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

cabi_new = pd.read_csv(data_dir + "species lists/gbif_matched/cabi_gbif.csv", dtype={"usageKey":str})
cabi_new = cabi_new.loc[cabi_new["New"]==True]

eppo_new = pd.read_csv(data_dir + "species lists/gbif_matched/eppo_gbif.csv", dtype={"usageKey":str})
eppo_new = eppo_new.loc[eppo_new["New"]==True]

sinas_new = pd.read_csv(data_dir + "species lists/gbif_matched/sinas_gbif.csv", dtype={"usageKey":str})
sinas_new = sinas_new.loc[sinas_new["New"]==True]

daisie_new = pd.read_csv(data_dir + "species lists/gbif_matched/daisie_gbif.csv", dtype={"usageKey":str})
daisie_new = daisie_new.loc[daisie_new["New"]==True]

# All SInAS assumed invasive

sinas_new["invasive"] = True
sinas_new.to_csv(data_dir + "species lists/new/sinas_new.csv", index=False)

# All DAISIE assumed invasive

daisie_new["invasive"] = True
daisie_new.to_csv(data_dir + "species lists/new/daisie_new.csv", index=False)

# EPPO - has categorizationd

print(f"Getting EPPO categorization for {len(eppo_new)} taxa")
eppo_new["categorization"] = eppo_new.codeEPPO.apply(lambda x: eppo_cat_api(x, token))
eppo_new["invasive"] = np.where(eppo_new["categorization"].map(len) > 0, True, False)

# Append and write to csv

eppo_gbif_inv = pd.read_csv(
    data_dir + "species lists/gbif_matched/eppo_gbif_with_categ.csv", dtype={"usageKey":str})
eppo_gbif_inv["New"] = False

eppo_gbif_inv = pd.concat([eppo_gbif_inv, eppo_new], ignore_index=True)
# Since these are all new from GBIF, should be mutually exlcusive

# Note: we may want to periodically check invasive == False values to see if the taxa have since become invasive

eppo_gbif_inv.to_csv(
    data_dir + "species lists/gbif_matched/eppo_gbif_with_categ.csv", index=False
)

eppo_new.to_csv(data_dir + "species lists/new/eppo_new.csv", index=False)

# CABI check occurs against the webscraped datasheet type (1 time)

cabi_new.to_csv(data_dir + "species lists/new/cabi_new.csv", index=False)

# Export a list (as .csv) of all new species usageKeys (to run the full EPPO and CABI queries on)

new_species = pd.concat([cabi_new, eppo_new, sinas_new], ignore_index=True)
new_species.loc[new_species["usageKey"].notna()]["usageKey"].drop_duplicates().to_csv(
    data_dir + "species lists/new/new_usageKeys.csv", index=False
)

print(
    f"Exported {len(new_species.loc[new_species['usageKey'].notna()])} new usageKeys, including"
    f" {len(new_species.loc[new_species['invasive']==True])} invasive taxa.")
