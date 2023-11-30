"""
File: data_update/1a2_integrate_unmatched.py
Author: Ariel Saffer
Date created: 2023-04-14
Description: Optional; After running the R script MatchUnmatched.R, append newly matched species to the gbif matched lists
"""

import pandas as pd
import os
import sys
import dotenv

from datetime import date

sys.path.append(os.getcwd())

dotenv.load_dotenv(".env")
data_dir = os.getenv("DATA_PATH")
token = os.getenv("EPPO_TOKEN")

# Get today's date
today = date.today()

# Bring back in the gbif matched data

cabi_gbif = pd.read_csv(data_dir + "species lists/gbif_matched/cabi_gbif.csv")
eppo_gbif = pd.read_csv(data_dir + "species lists/gbif_matched/eppo_gbif.csv")
asfr_gbif = pd.read_csv(data_dir + "species lists/gbif_matched/asfr_gbif.csv")

"""
#daisie_gbif = pd.read_csv(data_dir + "species lists/gbif_matched/daisie_gbif.csv")
# Keep only usageKeys that are not NA

cabi_gbif = cabi_gbif.loc[~cabi_gbif["usageKey"].isna()].reset_index(drop=True)
eppo_gbif = eppo_gbif.loc[~eppo_gbif["usageKey"].isna()].reset_index(drop=True)
asfr_gbif = asfr_gbif.loc[~asfr_gbif["usageKey"].isna()].reset_index(drop=True)
daisie_gbif = daisie_gbif.loc[~daisie_gbif["usageKey"].isna()].reset_index(drop=True)

# To handle the mix of numeric and text usageKeys
# turn usageKey into an integer, then a string
"""
cabi_gbif["usageKey"] = cabi_gbif["usageKey"].astype("Int64").astype(str)
eppo_gbif["usageKey"] = eppo_gbif["usageKey"].astype("Int64").astype(str)
asfr_gbif["usageKey"] = asfr_gbif["usageKey"].astype("Int64").astype(str)

# Bring in the newly matched data
"""
unmatched_records = pd.read_csv(
    data_dir + "species lists/gbif_matched/unmatched_records_usagekey_uid.csv"
).rename(columns={"usagekey": "usageKey"})
unmatched_records["New"] = True
unmatched_records["Date"] = f"{today.year}-{today.month:02d}-{today.day:02d}"

# Turn usageKey into an integer, then a string
unmatched_records["usageKey"] = (
    unmatched_records["usageKey"].astype("Int64").astype(str)
)

# If usageKey is NA, replace with uid

unmatched_records.loc[
    unmatched_records["usageKey"] == "<NA>", "usageKey"
] = unmatched_records.loc[unmatched_records["usageKey"] == "<NA>", "generate_uid"]

# Append the new matches by source

cabi_gbif = pd.concat(
    [
        cabi_gbif,
        unmatched_records.loc[
            unmatched_records["codeCABI"].notna(),
            ["codeCABI", "species", "usageKey", "New", "Date"],
        ],
    ],
    ignore_index=True,
)

eppo_gbif = pd.concat(
    [
        eppo_gbif,
        unmatched_records.loc[
            unmatched_records["codeEPPO"].notna(),
            ["codeEPPO", "species", "usageKey", "New", "Date"],
        ],
    ],
    ignore_index=True,
)
asfr_gbif = pd.concat(
    [
        asfr_gbif,
        unmatched_records.loc[
            unmatched_records["speciesASFR"].notna(),
            ["speciesASFR", "usageKey", "New", "Date"],
        ].rename(columns={"speciesASFR": "species"}),
    ],
    ignore_index=True,
)
"""
# Drop duplicates for each source, keeping the first

cabi_gbif = cabi_gbif.drop_duplicates(subset=["codeCABI"], keep="first")
eppo_gbif = eppo_gbif.drop_duplicates(subset=["codeEPPO"], keep="first")
asfr_gbif = asfr_gbif.drop_duplicates(subset=["species"], keep="first")

# Replace the files

cabi_gbif.to_csv(data_dir + "species lists/gbif_matched/cabi_gbif.csv", index=False)
eppo_gbif.to_csv(data_dir + "species lists/gbif_matched/eppo_gbif.csv", index=False)
asfr_gbif.to_csv(data_dir + "species lists/gbif_matched/asfr_gbif.csv", index=False)

# Report back
"""
print(
    f"Added {len(cabi_gbif.loc[cabi_gbif['usageKey'].str.startswith('XX')].index)} CABI species, "
    f"{len(eppo_gbif.loc[eppo_gbif['usageKey'].str.startswith('XX')].index)} EPPO species, "
    f"and {len(asfr_gbif.loc[asfr_gbif['usageKey'].str.startswith('XX')].index)} ASFR species."
    f"and {len(daisie_gbif.loc[daisie_gbif['usageKey'].str.startswith('XX')].index)} DAISIE species."
)
"""
