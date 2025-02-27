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

# SInAS - Keep all columns because they have the GBIF matches

sinas_species = pd.read_csv(
    data_dir + "species lists/by_database/sinas_full_list.csv",
)

# CABI

cabi_species = pd.read_csv(
    data_dir + "species lists/by_database/cabi_full_list.csv",
    usecols=["Scientific name", "URL"],
)

# Clean columns - codeSOURCE, name to match as 'species'
cabi_species["codeCABI"] = cabi_species.URL.str.extract("(\d+)")
cabi_species["codeCABI"] = cabi_species["codeCABI"].astype("int")
cabi_species.drop(columns="URL", inplace=True)

cabi_species.rename(columns={"Scientific name": "origTaxon"}, inplace=True)
eppo_species.rename(columns={"fullname": "origTaxon", "code": "codeEPPO"}, inplace=True)
daisie_species.rename(
    columns={"scientificName": "origTaxon", "idspecies": "codeDAISIE"}, inplace=True
)


# In the SInAS data, GBIF has already been matched
# Clean the GBIF column names by removing the GBIF prefix
sinas_species.rename(
    columns={colname: colname.replace("GBIF", "") for colname in sinas_species.columns},
    inplace=True,
)
sinas_species.rename(columns={"Taxon": "origTaxon"}, inplace=True)

# If we haven't matched them in GBIF previously, check for matches

# Read in the previous GBIF matches

try:
    sinas_gbif = pd.read_csv(
        data_dir + "species lists/gbif_matched/sinas_gbif.csv",
        dtype={"usageKey": "str"},
    )
except FileNotFoundError:
    sinas_gbif = pd.DataFrame(columns=["origTaxon", "usageKey", "New", "Date"])

try:
    cabi_gbif = pd.read_csv(
        data_dir + "species lists/gbif_matched/cabi_gbif.csv", dtype={"usageKey": "str"}
    )
except FileNotFoundError:
    cabi_gbif = pd.DataFrame(columns=["codeCABI", "usageKey", "New", "Date"])

try:
    eppo_gbif = pd.read_csv(
        data_dir + "species lists/gbif_matched/eppo_gbif.csv", dtype={"usageKey": "str"}
    )
except FileNotFoundError:
    eppo_gbif = pd.DataFrame(columns=["codeEPPO", "usageKey", "New", "Date"])

try:
    daisie_gbif = pd.read_csv(
        data_dir + "species lists/gbif_matched/daisie_gbif.csv",
        dtype={"usageKey": "str"},
    )
except FileNotFoundError:
    daisie_gbif = pd.DataFrame(columns=["codeDAISIE", "usageKey", "New", "Date"])

sinas_gbif["New"] = False
cabi_gbif["New"] = False
eppo_gbif["New"] = False
daisie_gbif["New"] = False

# Select matched values

sinas_match = sinas_gbif.loc[
    ~((sinas_gbif.usageKey.isna()) | (sinas_gbif.usageKey.str.startswith("X")))
]
cabi_match = cabi_gbif.loc[
    ~((cabi_gbif.usageKey.isna()) | (cabi_gbif.usageKey.str.startswith("X")))
]
eppo_match = eppo_gbif.loc[
    ~((eppo_gbif.usageKey.isna()) | (eppo_gbif.usageKey.str.startswith("X")))
]
daisie_match = daisie_gbif.loc[
    ~((daisie_gbif.usageKey.isna()) | (daisie_gbif.usageKey.str.startswith("X")))
]

# This will capture both new values and values that were not previously found in GBIF
# in case GBIF is able to match species that were previously missed

sinas_new = sinas_species.loc[
    ~sinas_species["origTaxon"].isin(sinas_match["taxonSINAS"])
]
cabi_new = cabi_species.loc[~cabi_species["codeCABI"].isin(cabi_match["codeCABI"])]
eppo_new = eppo_species.loc[~eppo_species["codeEPPO"].isin(eppo_match["codeEPPO"])]
daisie_new = daisie_species.loc[
    ~daisie_species["codeDAISIE"].isin(daisie_match["codeDAISIE"])
]

# Query GBIF for new names

print(f"Getting GBIF matches for {len(cabi_new)} CABI species...")
gbif_species_match(cabi_new)
cabi_new["Date"] = f"{today.year}-{today.month:02d}-{today.day:02d}"
cabi_new["New"] = True
cabi_gbif = pd.concat([cabi_gbif, cabi_new], ignore_index=True)
cabi_gbif.to_csv(data_dir + "species lists/gbif_matched/cabi_gbif.csv", index=False)
print("Exported CABI GBIF matches.")

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

# SInAS has already been matched
sinas_gbif = pd.concat([sinas_gbif, sinas_new], ignore_index=True)
sinas_gbif.rename(columns={"origTaxon": "taxonSINAS"}, inplace=True)
sinas_gbif[
    [
        "taxonSINAS",
        "usageKey",
        "scientificName",
        "taxonRank",
        "matchtype",
        "Date",
        "New",
    ]
].to_csv(data_dir + "species lists/gbif_matched/sinas_gbif.csv", index=False)
print("Exported SInAS GBIF matches.")

# Export the new unmatched species to csv

new_unmatched = pd.concat(
    [
        cabi_new.loc[cabi_new["usageKey"].isna()],
        sinas_new.loc[sinas_new["usageKey"].isna()],
        eppo_new.loc[eppo_new["usageKey"].isna()],
        daisie_new.loc[daisie_new["usageKey"].isna()],
    ],
    ignore_index=True,
)[["origTaxon", "codeCABI", "codeEPPO", "codeDAISIE"]]

new_unmatched["New"] = True
new_unmatched["Date"] = f"{today.year}-{today.month:02d}-{today.day:02d}"

# No need to append to exisitng - overwrite

new_unmatched.to_csv(
    data_dir + "species lists/gbif_matched/all_unmatched_gbif.csv", index=False
)
print(
    f"Exported {len(new_unmatched.index)} unmatched species to all_unmatched_gbif.csv."
)
