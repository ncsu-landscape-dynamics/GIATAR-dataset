"""
File: data_update/1a2_check_unfound_gbif_keys.py
Author: Thom Worm
Date created: 2024-06-01
Description: This script checks for species that were not found in the GBIF backbone and
attempts to match them to the GBIF backbone using the pygbif package. It then updates the
species lists with the new GBIF keys and taxonomic information.
"""

import pandas as pd
import os
import sys
import dotenv
import pygbif.species as gbif
import pytaxize.gn as gn
import numpy as np

sys.path.append(os.getcwd())

dotenv.load_dotenv(".env")
data_dir = os.getenv("DATA_PATH")

# Import data functions
from data_update.data_functions import check_gbif_tax_secondary, update_GBIFstatus


# Bring in new species lists

gbif_backbone = pd.read_csv(
    data_dir + "\GBIF data\GBIF_backbone_invasive.csv", dtype={"usageKey": str}
)
cabi_gbif = pd.read_csv(
    data_dir + "\species lists\gbif_matched\cabi_gbif.csv", dtype={"usageKey": str}
)
eppo_gbif = pd.read_csv(
    data_dir + "\species lists\gbif_matched\eppo_gbif.csv", dtype={"usageKey": str}
)
sinas_gbif = pd.read_csv(
    data_dir + "\species lists\gbif_matched\sinas_gbif.csv", dtype={"usageKey": str}
)
daisie_gbif = pd.read_csv(
    data_dir + "\species lists\gbif_matched\daisie_gbif.csv", dtype={"usageKey": str}
)

# if matchType is NA set to ""
sinas_gbif["matchType"] = sinas_gbif["matchType"].replace(np.nan, "", regex=True)

data_files = [sinas_gbif, cabi_gbif, eppo_gbif, daisie_gbif]

specie_unfound = []
for file in data_files:
    for index, row in file.iterrows():
        if row["matchType"] in ["", "NA", "HIGHERRANK"]:
            specie_unfound.append(row["origTaxon"])
# drop duplicates
specie_unfound = list(set(specie_unfound))

check_species_df = pd.DataFrame(specie_unfound, columns=["origTaxon"])

matched_species, unmatched = check_gbif_tax_secondary(check_species_df)
# make dc into dataframe
matched_species = pd.DataFrame(matched_species)

matched_species = matched_species.apply(update_GBIFstatus, axis=1)
# write matched_species to csv as previously_unmatched_species.csv
matched_species.to_csv(
    data_dir + "\species lists\previously_unmatched_species_gbif_match_sinas.csv"
)

for file in data_files:

    if "kingdom" not in file.columns:
        file["kingdom"] = None
        file["phylum"] = None
        file["class"] = None
        file["order"] = None
        file["family"] = None

        file["genus"] = None
        file["GBIFstatus"] = None
        file["GBIFtaxonRank"] = None
        file["taxonomic_species"] = None
    if "canonicalName" not in file.columns:
        file["canonicalName"] = None
    # if usageKey col is not string, set to string
    if file["usageKey"].dtype != "str":
        file["usageKey"] = file["usageKey"].astype(str)
    # remove any .0 caused by floats
    file["usageKey"] = file["usageKey"].str.replace(".0", "", regex=False)

    for index, row in file.iterrows():

        skip_uk = 0
        if row["matchType"] in ["", "NA", "HIGHERRANK"]:
            # search for species in matched_species
            search = matched_species[matched_species["origTaxon"] == row["origTaxon"]]
            if search.empty:
                print(row["origTaxon"])
                # break
                print(row)
                continue
            elif (
                search.iloc[0]["GBIFstatus"] == "Missing"
                or search.iloc[0]["GBIFstatus"] == None
            ):
                print("making XX")
                if file.at[index, "matchType"] == "HIGHERRANK":
                    print("HIGHERRANK")
                    print(file.at[index, "origTaxon"])
                try:
                    file.at[index, "usageKey"] = "XX" + row["origTaxon"].replace(
                        " ", "_"
                    )
                    print("creating UK" + "XX" + row["origTaxon"].replace(" ", "_"))
                except AttributeError:
                    print(row["origTaxon"])
                    pass

                continue
            else:
                skip_uk = 1
                search = search.iloc[0]
            file.at[index, "kingdom"] = search["kingdom"]
            file.at[index, "phylum"] = search["phylum"]
            file.at[index, "class"] = search["class"]
            file.at[index, "order"] = search["order"]
            file.at[index, "family"] = search["family"]
            file.at[index, "genus"] = search["genus"]
            file.at[index, "usageKey"] = search["GBIFusageKey"]
            file.at[index, "canonicalName"] = search["Taxon"]
            file.at[index, "scientificName"] = search["scientificName"]
            file.at[index, "matchType"] = search["GBIFmatchtype"]
            file.at[index, "rank"] = search["GBIFtaxonRank"]
            file.at[index, "taxonomic_species"] = search["species"]
        else:
            # match to gbif_backbone
            search = gbif_backbone[gbif_backbone["usageKey"] == row["usageKey"]]

            if search.empty:
                print("no match to gbif backbone")
                if (
                    pd.isnull(file.at[index, "usageKey"])
                    or file.at[index, "usageKey"] == ""
                    or row["usageKey"] == "nan"
                ):

                    print("making XX")
                    if file.at[index, "matchType"] == "HIGHERRANK":
                        print("HIGHERRANK")
                        print(file.at[index, "origTaxon"])
                    try:
                        file.at[index, "usageKey"] = "XX" + row["origTaxon"].replace(
                            " ", "_"
                        )
                        print("creating UK" + "XX" + row["origTaxon"].replace(" ", "_"))
                    except AttributeError:
                        print(row["origTaxon"])
                        pass

                continue
            else:
                search = search.iloc[0]
                # print(search)
                file.at[index, "kingdom"] = search["kingdom"]
                file.at[index, "phylum"] = search["phylum"]
                file.at[index, "class"] = search["class"]
                file.at[index, "order"] = search["order"]
                file.at[index, "family"] = search["family"]
                file.at[index, "genus"] = search["genus"]
                file.at[index, "gbif_species"] = search["species"]
                # if cannonical name blank
                if pd.isnull(row["canonicalName"]):
                    file.at[index, "canonicalName"] = search["species"]
                    file.at[index, "scientificName"] = search["scientificName"]
                    file.at[index, "matchType"] = search["taxonomicStatus"]
                    file.at[index, "rank"] = search["taxonRank"]
                    file.at[index, "taxonomic_species"] = search["species"]

        if (
            pd.isnull(file.at[index, "usageKey"])
            or file.at[index, "matchType"] == "HIGHERRANK"
            or file.at[index, "usageKey"] == "NA"
            or file.at[index, "usageKey"] == ""
            or pd.isnull(row["usageKey"])
        ):
            print("making XX")
            if file.at[index, "matchType"] == "HIGHERRANK":
                print("HIGHERRANK")
                print(file.at[index, "origTaxon"])
            try:
                file.at[index, "usageKey"] = "XX" + row["origTaxon"].replace(" ", "_")
                print("creating UK" + "XX" + row["origTaxon"].replace(" ", "_"))
            except AttributeError:
                print(row["origTaxon"])
        elif (
            pd.isnull(row["usageKey"])
            or row["usageKey"] == ""
            or row["usageKey"] == "NaN"
            or row["usageKey"] == "nan"
        ):
            print("NA")
            print(row["origTaxon"])
            print(row["usageKey"])
            print(file.at[index, "usageKey"])
            print(file.at[index, "origTaxon"])
        else:
            print(row["usageKey"])

sinas_gbif_match = data_files[0]
cabi_gbif_match = data_files[1]
eppo_gbif_match = data_files[2]
daisie_gbif_match = data_files[3]

# Last cleaning:
# Drop the column "Unnamed: 0" if it exists
# Ensure that all usageKeys are strings and remove ".0" from former floats

for df in [cabi_gbif_match, eppo_gbif_match, sinas_gbif_match, daisie_gbif_match]:
    if "Unnamed: 0" in df.columns:
        df.drop(columns=["Unnamed: 0"], inplace=True)
    df["usageKey"] = df["usageKey"].astype(str)
    df["usageKey"] = df["usageKey"].str.replace(".0", "", regex=False)

# write to csv
cabi_gbif_match.to_csv(
    data_dir + "\species lists\gbif_matched\cabi_gbif.csv", index=False
)
eppo_gbif_match.to_csv(
    data_dir + "\species lists\gbif_matched\eppo_gbif.csv", index=False
)
daisie_gbif_match.to_csv(
    data_dir + "\species lists\gbif_matched\daisie_gbif.csv", index=False
)
sinas_gbif_match.to_csv(
    data_dir + "\species lists\gbif_matched\sinas_gbif.csv", index=False
)
