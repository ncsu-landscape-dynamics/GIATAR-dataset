"""
File: data_update/3d_process_daisie_data.py
Author: Thom Worm
Date created: 2023-04-14
Description: Pre-process the DAISIE datasets into a clean format
"""

import pandas as pd
import numpy as np
import os
import dotenv
from data_functions import *

dotenv.load_dotenv(".env")
data_dir = os.getenv("DATA_PATH")

# Download all files in the DAISIE Github repository "raw" directory:
# https://github.com/trias-project/daisie-checklist/tree/master/data/raw
# Save to DAISIE data/raw

# read master list of usageKeys

DAISIE_sources = pd.read_csv(
    data_dir + "/DAISIE data/raw/DAISIE_literature_references.csv", low_memory=False
)
DAISIE_sources = DAISIE_sources[["sourceid", "longref"]].drop_duplicates()
DAISIE_sources_dict = dict(zip(DAISIE_sources["sourceid"], DAISIE_sources["longref"]))
DAISIE_species = pd.read_csv(
    data_dir + "/DAISIE data/raw/DAISIE_taxon.csv", low_memory=False
)
# create column genus_species for DAISIE
DAISIE_species["genus_species"] = (
    DAISIE_species["genus"] + " " + DAISIE_species["species"]
)
# create column source that is sourceid mapped to daiise_sources_dict
DAISIE_species["source"] = DAISIE_species["sourceid"].map(DAISIE_sources_dict)

# replace species with genus_species in daiise_species and remove genus
DAISIE_species["species"] = DAISIE_species["genus_species"]
DAISIE_species = DAISIE_species.drop(columns=["genus_species", "genus"])
# gbif_species_match(DAISIE_species)
master_usageKey = pd.read_csv(
    data_dir + "/species lists/invasive_all_source.csv", low_memory=False
)
# create column usageKey for DAISIE species and pull usageKeys from master list where species column in species matches taxonDAISIE in master do it iteratively because of duplicates
DAISIE_species["usageKey"] = np.nan
for i in range(len(DAISIE_species)):
    species = DAISIE_species.iloc[i]["species"]

    if species in master_usageKey["taxonDAISIE"].values:
        usageKey = master_usageKey[master_usageKey["taxonDAISIE"] == species][
            "usageKey"
        ].values[0]
        DAISIE_species.at[i, "usageKey"] = usageKey


DAISIE_species = DAISIE_species.dropna(subset=["usageKey"])

# for species where usagekey is na, make usagekey "XX" + "DAI" "first 3 letters of first word of species" + "first three letters of second word of species"
# unless spece is blank then make it "XXDAI" plus first three letters of taxon_group plus sourceid passed to int  to string

# DAISIE_species['usageKey'] = np.where(DAISIE_species['usageKey'].isna(), 'XXDAI' + DAISIE_species['species'].str.split().str[0].str[:3] + DAISIE_species['species'].str.split().str[1].str[:3], DAISIE_species['usageKey'])
# Fill NaN values in 'species' column (if any) with an empty string or any desired default value


DAISIE_species_usageKey = DAISIE_species[["idspecies", "usageKey"]].drop_duplicates()
# make a dictionary of idspecies:usageKey pairs
DAISIE_species_usageKey_dict = dict(
    zip(DAISIE_species_usageKey["idspecies"], DAISIE_species_usageKey["usageKey"])
)
###############################################################################################
# DAISIE_donor_areas
donor_area = pd.read_csv(
    data_dir + "/DAISIE data/raw/DAISIE_donor_area.csv", low_memory=False
)
donor_area["source"] = np.where(
    donor_area["sourceid"].isnull(),
    "DAISIE",
    donor_area["sourceid"].map(DAISIE_sources_dict),
)
donor_area["usageKey"] = donor_area["idspecies"].map(DAISIE_species_usageKey_dict)
donor_area = donor_area.drop(columns=["sourceid"])
donor_area = donor_area.rename(columns={"idspecies": "DAISIE_idspecies"})
donor_area = donor_area.drop_duplicates()
donor_area = donor_area.rename(columns={"region": "donor_region"})
donor_area.to_csv(data_dir + "/DAISIE data/DAISIE_donor_area.csv", index=False)

# daisie habitat
habitat = pd.read_csv(
    data_dir + "/DAISIE data/raw/DAISIE_habitat.csv", low_memory=False
)
# if sourceid is null, set to 'DAISIE', otherwise map to sourceid:longref dictionary
habitat["source"] = np.where(
    habitat["sourceid"].isnull(), "DAISIE", habitat["sourceid"].map(DAISIE_sources_dict)
)
# create column usageKey by mapping idspecies to usageKey in DAISIE_species_usageKey_dict
habitat["usageKey"] = habitat["idspecies"].map(DAISIE_species_usageKey_dict)
# drop  sourceid columns
habitat = habitat.drop(columns=["sourceid"])
# rename idspecies to DAISIE_idspecies
habitat = habitat.rename(columns={"idspecies": "DAISIE_idspecies"})
# write to csv in DAIISIE data/
habitat.to_csv(data_dir + "/DAISIE data/DAISIE_habitat.csv", index=False)


# daisie pathways
pathways = pd.read_csv(
    data_dir + "/DAISIE data/raw/DAISIE_pathways.csv", low_memory=False
)
# if sourceid is null, set to 'DAISIE', otherwise map to sourceid:longref dictionary
pathways["source"] = np.where(
    pathways["sourceid"].isnull(),
    "DAISIE",
    pathways["sourceid"].map(DAISIE_sources_dict),
)
# create column usageKey by mapping idspecies to usageKey in DAISIE_species_usageKey_dict
pathways["usageKey"] = pathways["idspecies"].map(DAISIE_species_usageKey_dict)
# drop  sourceid columns
pathways = pathways.drop(columns=["sourceid"])
# rename idspecies to DAISIE_idspecies
pathways = pathways.rename(columns={"idspecies": "DAISIE_idspecies"})
# drop id_sp_reg
pathways = pathways.drop(columns=["id_sp_region"])

# write to csv in DAIISIE data/
pathways.to_csv(data_dir + "/DAISIE data/DAISIE_pathways.csv", index=False)


# daisie vectors
vectors = pd.read_csv(
    data_dir + "/DAISIE data/raw/DAISIE_vectors.csv", low_memory=False
)
# if sourceid is null, set to 'DAISIE', otherwise map to sourceid:longref dictionary
vectors["source"] = np.where(
    vectors["sourceid"].isnull(), "DAISIE", vectors["sourceid"].map(DAISIE_sources_dict)
)
# create column usageKey by mapping idspecies to usageKey in DAISIE_species_usageKey_dict
vectors["usageKey"] = vectors["idspecies"].map(DAISIE_species_usageKey_dict)
# drop  sourceid columns
vectors = vectors.drop(columns=["sourceid"])
# rename idspecies to DAISIE_idspecies
vectors = vectors.rename(columns={"idspecies": "DAISIE_idspecies"})
# drop id_sp_reg
vectors = vectors.drop(columns=["id_sp_region"])
# write to csv in DAIISIE data/
vectors.to_csv(data_dir + "/DAISIE data/DAISIE_vectors.csv", index=False)


# vernacular names
vernacular = pd.read_csv(
    data_dir + "/DAISIE data/raw/DAISIE_vernacular_names.csv", low_memory=False
)
# if sourceid is null, set to 'DAISIE', otherwise map to sourceid:longref dictionary
vernacular["source"] = np.where(
    vernacular["sourceid"].isnull(),
    "DAISIE",
    vernacular["sourceid"].map(DAISIE_sources_dict),
)
# create column usageKey by mapping idspecies to usageKey in DAISIE_species_usageKey_dict
vernacular["usageKey"] = vernacular["idspecies"].map(DAISIE_species_usageKey_dict)
# drop  sourceid columns
vernacular = vernacular.drop(columns=["sourceid"])
# rename idspecies to DAISIE_idspecies
vernacular = vernacular.rename(columns={"idspecies": "DAISIE_idspecies"})
# write to csv in DAIISIE data/
vernacular.to_csv(data_dir + "/DAISIE data/DAISIE_vernacular_names.csv", index=False)

# daisie distribution
distribution = pd.read_csv(
    data_dir + "/DAISIE data/raw/DAISIE_distribution.csv", low_memory=False
)
# if sourceid is null, set to 'DAISIE', otherwise map to sourceid:longref dictionary
# distribution['source'] = np.where(distribution['sourceid'].isnull(), 'DAISIE', distribution['sourceid'].map(DAISIE_sources_dict))
# create column usageKey by mapping idspecies to usageKey in DAISIE_species_usageKey_dict
# distribution['usageKey'] = distribution['idspecies'].map(DAISIE_species_usageKey_dict)
# drop  sourceid columns
# distribution = distribution.drop(columns=[ 'sourceid'])
# rename idspecies to DAISIE_idspecies
# create column source that is id_sp_region mapped to daiise_region_sources_dict
# reset index to get rid of duplicate index
# DAISIE_region_sources_dict['nan'] = 'DAISIE'
"""
distribution = distribution.drop_duplicates()

#if id_sp_region is null, set to 'DAISIE', otherwise map to id_sp_region:longref dictionary
distribution['source'] = np.where(distribution['id_sp_region'].isnull(), 'DAISIE', 
#distribution['id_sp_region'].map(DAISIE_region_sources_dict))


distribution = distribution.rename(columns={'idspecies': 'DAISIE_idspecies'})
#write to csv in DAIISIE data/
distribution.to_csv(data_dir + "/DAISIE data/DAISIE_distribution.csv", index=False)
"""
