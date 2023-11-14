"""
File: data_update/1a_new_species_gbif_match.py
Author: Ariel Saffer
Date created: 2023-04-14
Description: Match the newly added species to their GBIF usage keys
"""

import pandas as pd
import os
import sys
import dotenv
from glob import glob

from datetime import date

sys.path.append(os.getcwd())

from data_update.data_functions import gbif_species_match

dotenv.load_dotenv(".env")
data_dir = os.getenv("DATA_PATH")
token = os.getenv("EPPO_TOKEN")

# Get today's date
today = date.today()

# Read in the four data sets 
# EPPO

eppo_species = pd.read_csv(
    data_dir + "species lists/by_database/eppo_full_list.csv",
    usecols=["fullname", "code"],
)

# DAISIE

daisie_species = pd.read_csv(
    data_dir + "species lists/by_database/daisie_full_list.csv",
    usecols=["scientificName", "idspecies"],
)

# Read the most recent version of these two: ASFR (Seebens' Alien Species First Records), CABI export

asfr_species = pd.read_csv(
    sorted(
        glob(data_dir + "species lists/by_database/asfr_full_list.csv"),
        key=os.path.getmtime,
    )[-1],
    usecols=["scientificName"],
).drop_duplicates()

cabi_species = pd.read_csv(
    sorted(
        glob(data_dir + "species lists/by_database/cabi_full_list.csv"),
        key=os.path.getmtime,
    )[-1],
    usecols=["Scientific name", "URL"],
)

# Clean columns - codeSOURCE, name to match as 'species'
cabi_species["codeCABI"] = cabi_species.URL.str.extract("(\d+)")
cabi_species["codeCABI"] = cabi_species["codeCABI"].astype("int")
cabi_species.drop(columns="URL", inplace=True)

cabi_species.rename(columns={"Scientific name": "species"}, inplace=True)
asfr_species.rename(columns={"scientificName": "species"}, inplace=True)
eppo_species.rename(columns={"fullname": "species", "code": "codeEPPO"}, inplace=True)
daisie_species.rename(columns={"scientificName": "species", "idspecies": "codeDAISIE"}, inplace=True)

# If we haven't matched them in GBIF previously, check for matches

# Get existing GBIF matches (if they already exist)
try: 
    asfr_gbif = pd.read_csv(data_dir + "species lists/gbif_matched/asfr_gbif.csv", dtype={"usageKey": "str"})
except FileNotFoundError:
    asfr_gbif = pd.DataFrame(columns=["species", "usageKey", "New", "Date"])

try:
    cabi_gbif = pd.read_csv(data_dir + "species lists/gbif_matched/cabi_gbif.csv", dtype={"usageKey": "str"})
except FileNotFoundError:
    cabi_gbif = pd.DataFrame(columns=["codeCABI", "usageKey", "New", "Date"])

try:
    eppo_gbif = pd.read_csv(data_dir + "species lists/gbif_matched/eppo_gbif.csv", dtype={"usageKey": "str"})
except FileNotFoundError:
    eppo_gbif = pd.DataFrame(columns=["codeEPPO", "usageKey", "New", "Date"])

try: 
    daisie_gbif = pd.read_csv(data_dir + "species lists/gbif_matched/daisie_gbif.csv", dtype={"usageKey": "str"})
except FileNotFoundError:
    daisie_gbif = pd.DataFrame(columns=["codeDAISIE", "usageKey", "New", "Date"])

asfr_gbif["New"] = False
cabi_gbif["New"] = False
eppo_gbif["New"] = False
daisie_gbif["New"] = False

# Select matched values

asfr_match = asfr_gbif.loc[~((asfr_gbif.usageKey.isna()) | (asfr_gbif.usageKey.str.startswith("X")))]
cabi_match = cabi_gbif.loc[~((cabi_gbif.usageKey.isna()) | (cabi_gbif.usageKey.str.startswith("X")))]
eppo_match = eppo_gbif.loc[~((eppo_gbif.usageKey.isna()) | (eppo_gbif.usageKey.str.startswith("X")))]
daisie_match = daisie_gbif.loc[~((daisie_gbif.usageKey.isna()) | (daisie_gbif.usageKey.str.startswith("X")))]

# This will capture both new values and values that were not previously found in GBIF
# in case GBIF is able to match species that were previously missed

cabi_new = cabi_species.loc[~cabi_species["codeCABI"].isin(cabi_match["codeCABI"])]
asfr_new = asfr_species.loc[~asfr_species["species"].isin(asfr_match["species"])]
eppo_new = eppo_species.loc[~eppo_species["codeEPPO"].isin(eppo_match["codeEPPO"])]
daisie_new = daisie_species.loc[~daisie_species["codeDAISIE"].isin(daisie_match["codeDAISIE"])]

# Query GBIF for new names

print(f"Getting GBIF matches for {len(cabi_new)} CABI species...")
gbif_species_match(cabi_new)
cabi_new["Date"] = f"{today.year}-{today.month:02d}-{today.day:02d}"
cabi_new["New"] = True
cabi_gbif = pd.concat([cabi_gbif, cabi_new], ignore_index=True)
cabi_gbif.to_csv(data_dir + "species lists/gbif_matched/cabi_gbif.csv", index=False)
print("Exported CABI GBIF matches.")

print(f"Getting GBIF matches for {len(asfr_new)} ASFR species...")
gbif_species_match(asfr_new)
asfr_new["Date"] = f"{today.year}-{today.month:02d}-{today.day:02d}"
asfr_new["New"] = True
asfr_gbif = pd.concat([asfr_gbif, asfr_new], ignore_index=True)
asfr_gbif.to_csv(data_dir + "species lists/gbif_matched/asfr_gbif.csv", index=False)
print("Exported ASFR GBIF matches.")

print(f"Getting GBIF matches for {len(eppo_new)} EPPO species...")
gbif_species_match(eppo_new)
eppo_new["Date"] = f"{today.year}-{today.month:02d}-{today.day:02d}"
eppo_new["New"] = True
eppo_gbif = pd.concat([eppo_gbif, eppo_new], ignore_index=True)
eppo_gbif.to_csv(data_dir + "species lists/gbif_matched/eppo_gbif.csv", index=False)
print("Exported EPPO GBIF matches.")

print(f"Getting GBIF matches for {len(daisie_new)} DAISIE species...")
gbif_species_match(daisie_new)
daisie_new["Date"] = f"{today.year}-{today.month:02d}-{today.day:02d}"
daisie_new["New"] = True
daisie_gbif = pd.concat([daisie_gbif, daisie_new], ignore_index=True)
daisie_gbif.to_csv(data_dir + "species lists/gbif_matched/daisie_gbif.csv", index=False)
print("Exported DAISIE GBIF matches.")

# Export the new unmatched species to csv

new_unmatched = pd.concat([
    cabi_new.loc[cabi_new["usageKey"].isna()],
    asfr_new.loc[asfr_new["usageKey"].isna()],
    eppo_new.loc[eppo_new["usageKey"].isna()],
    daisie_new.loc[daisie_new["usageKey"].isna()]
    ], ignore_index=True)[["scientificName", "species", "codeCABI", "codeEPPO", "codeDAISIE"]]

new_unmatched["New"] = True
new_unmatched["Date"] = f"{today.year}-{today.month:02d}-{today.day:02d}"

# Append to the exisiting file if it exists

try:
    unmatched_records = pd.read_csv(data_dir + "species lists/gbif_matched/all_unmatched_gbif.csv")
    unmatched_records["New"] = False
    unmatched_records = pd.concat([unmatched_records, new_unmatched], ignore_index=True)
except FileNotFoundError:
    unmatched_records = new_unmatched

unmatched_records.to_csv(data_dir + "species lists/gbif_matched/all_unmatched_gbif.csv", index=False)
