"""
File: data_update/1c_combine_species_lists.py
Author: Ariel Saffer
Date created: 2023-04-14
Description: Combine the species lists from ASFR, CABI, and EPPO using the GBIF usageKey
"""

import pandas as pd
import os
import sys
import dotenv

sys.path.append(os.getcwd())

dotenv.load_dotenv(".env")
data_dir = os.getenv("DATA_PATH")

# Bring in new species lists

asfr_gbif = pd.read_csv(data_dir + "species lists/gbif_matched/asfr_gbif.csv", dtype={"usageKey":str})
cabi_gbif = pd.read_csv(data_dir + "species lists/gbif_matched/cabi_gbif.csv", dtype={"usageKey":str})
eppo_gbif = pd.read_csv(data_dir + "species lists/gbif_matched/eppo_gbif.csv", dtype={"usageKey":str})
daisie_gbif = pd.read_csv(data_dir + "species lists/gbif_matched/daisie_gbif.csv", dtype={"usageKey":str})

# Specific row names

cabi_gbif.rename(columns={"species": "speciesCABI"}, inplace=True)
cabi_gbif["source"] = "CABI"

asfr_gbif.rename(columns={"species": "speciesASFR"}, inplace=True)
asfr_gbif["source"] = "ASFR"

eppo_gbif.rename(columns={"species": "speciesEPPO"}, inplace=True)
eppo_gbif["source"] = "EPPO"

daisie_gbif.rename(columns={"species": "speciesDAISIE"}, inplace=True)
daisie_gbif["source"] = "DAISIE"

# Keep the matches

cabi_match = cabi_gbif.loc[cabi_gbif["usageKey"].notna()]
asfr_match = asfr_gbif.loc[asfr_gbif["usageKey"].notna()]
eppo_match = eppo_gbif.loc[eppo_gbif["usageKey"].notna()]
daisie_match = daisie_gbif.loc[daisie_gbif["usageKey"].notna()]

# EPPO - read in already categorized data

eppo_gbif_inv = pd.read_csv(
    data_dir + "species lists/gbif_matched/eppo_gbif_with_categ.csv"
)

# Rename columns
eppo_gbif_inv.rename(columns={"invasive": "invasiveEPPO"}, inplace=True)

# Merge into the eppo_match dataset

eppo_match = pd.merge(
    left=eppo_match,
    right=eppo_gbif_inv.loc[:, ["codeEPPO", "invasiveEPPO"]],
    how="left",
    on="codeEPPO",
)

# CABI already matched to invasive

cabi_gbif_inv = pd.read_csv(
    data_dir + "species lists/gbif_matched/CABI_invasive_TF.csv"
)
cabi_gbif_inv.rename(columns={"invasive": "invasiveCABI"}, inplace=True)

# Merge into the cabi_match dataset

cabi_match = pd.merge(
    left=cabi_match,
    right=cabi_gbif_inv.loc[:, ["code", "invasiveCABI"]],
    how="left",
    left_on="codeCABI",
    right_on="code",
)

# Create a separate df of just the GBIF data - usage key: species info

gbif_records = (
    pd.concat(
        [
            cabi_match.loc[:, "usageKey":"rank"],
            asfr_match.loc[:, "usageKey":"rank"],
            eppo_match.loc[:, "usageKey":"rank"],
            daisie_match.loc[:, "usageKey":"rank"],
        ],
        axis=0,
    )
    .reset_index()
    .drop(columns=["index"])
)
gbif_records = gbif_records.drop_duplicates("usageKey").sort_index()


# Rebuild a combined dataframe: for each usageKey, map records from CABI, ASFR, and EPPO

combined_records = pd.merge(
    left=cabi_match.loc[:, ["speciesCABI", "codeCABI", "usageKey", "invasiveCABI"]],
    right=asfr_match.loc[:, "speciesASFR":"usageKey"],
    how="outer",
    on="usageKey",
)

combined_records = pd.merge(
    left=combined_records,
    right=eppo_match.loc[:, ["speciesEPPO", "codeEPPO", "usageKey", "invasiveEPPO"]],
    how="outer",
    on="usageKey",
)

combined_records = pd.merge(
    left=combined_records,
    right=daisie_match.loc[:, ["speciesDAISIE", "codeDAISIE", "usageKey"]],
    how="outer",
    on="usageKey",
)

# Add back in one copy of the GBIF data

combined_records = pd.merge(
    left=combined_records, right=gbif_records, how="left", on="usageKey",
)

# Any species that appears in ASFR, has a categorization in EPPO, or is an Invasive species/Pest/Pest vector datasheet type in CABI

invasive_all = (
    combined_records.loc[
        (combined_records["invasiveEPPO"] == True)
        | (combined_records["invasiveCABI"] == "True")
        | (combined_records["speciesASFR"].notna())
        | (combined_records["speciesDAISIE"].notna())
    ]
    .reset_index()
    .drop(columns="index")
)

# Drop duplicated rows 
invasive_all = invasive_all.drop_duplicates(keep="first")

# Write to csv

invasive_all.to_csv(data_dir + "species lists/invasive_all_source.csv", index=False)

print("Saved invasive all source file.")

# Make the link files

ASFR_link = invasive_all.loc[
    invasive_all["speciesASFR"].notna(), ["usageKey", "speciesASFR"]
].reset_index(drop=True).drop_duplicates()
EPPO_link = invasive_all.loc[
    invasive_all["codeEPPO"].notna(), ["usageKey", "speciesEPPO", "codeEPPO"]
].reset_index(drop=True).drop_duplicates()
CABI_link = invasive_all.loc[
    invasive_all["codeCABI"].notna(), ["usageKey", "speciesCABI", "codeCABI"]
].reset_index(drop=True).drop_duplicates()
DAISIE_link = invasive_all.loc[
    invasive_all["codeDAISIE"].notna(), ["usageKey", "speciesDAISIE", "codeDAISIE"]
].reset_index(drop=True).drop_duplicates()

GBIF_backbone = invasive_all[["usageKey"]].drop_duplicates().reset_index(drop=True)

# Write to CSV

ASFR_link.to_csv(data_dir + "link files/ASFR_link.csv", index=False)
EPPO_link.to_csv(data_dir + "link files/EPPO_link.csv", index=False)
CABI_link.to_csv(data_dir + "link files/CABI_link.csv", index=False)
DAISIE_link.to_csv(data_dir + "link files/DAISIE_link.csv", index=False)

GBIF_backbone.to_csv(data_dir + "link files/GBIF_db_backbone.csv", index=False)

print("Saved all link files.")

# Replace the GBIF invasive taxonomy backbone file

gbif_all = pd.read_csv(data_dir + "species lists/by_database/gbif_all_small.csv", sep='\t')

# Rename taxonKey to usageKey

gbif_all.rename(columns={"taxonKey": "usageKey"}, inplace=True)

# Set datatype to string

gbif_all["usageKey"] = gbif_all["usageKey"].astype("int64").astype(str)

# Filter gbif_all to usageKeys in invasive_link

gbif_invasive = gbif_all.loc[gbif_all["usageKey"].isin(GBIF_backbone["usageKey"])]

# Save to csv 

gbif_invasive.to_csv(data_dir + "GBIF data/GBIF_backbone_invasive.csv", index=False)