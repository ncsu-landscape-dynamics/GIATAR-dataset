"""
File: data_update/4_consolidate_all_occurence.py
Author: Ariel Saffer
Date created: 2023-04-14
Description: Re-consolidate all occurrence records and first records after getting new content from other sources
"""

import pandas as pd
import numpy as np
import os
import dotenv
import sys
import re

sys.path.append(os.getcwd())

from data_update.data_functions import get_ISO3, clean_DAISIE_year, match_countries

# Get data dir - invasive database folder
dotenv.load_dotenv(".env")
data_dir = os.getenv("DATA_PATH")

### CABI DATA

print("Reading and cleaning CABI data...")

# Read file
CABI_occur = pd.read_csv(
    data_dir + "CABI data/CABI_tables/todistributionDatabaseTable.csv",
    # usageKey column as string
    dtype={"usageKey": "str"},
)

CABI_occur = CABI_occur.loc[
    (CABI_occur["First Reported"] != CABI_occur["Distribution"])
][
    [
        "Continent/Country/Region",
        "Distribution",
        "Origin",
        "Last Reported",
        "First Reported",
        "code",
        "usageKey",
        "Reference",
    ]
]

# Filtering to entries that (1) have dates and (2) indicate presence
# Keeping: non-header rows, at least one date (First/Last reported), relevant columns
CABI_present = CABI_occur.loc[CABI_occur["Distribution"].str.find("Present") != -1]

# Get one year per record
CABI_present["Last Reported"] = pd.to_numeric(CABI_present["Last Reported"])
CABI_present["First Reported"] = pd.to_numeric(CABI_present["First Reported"])
CABI_present["year"] = CABI_present[["Last Reported", "First Reported"]].min(axis=1)

# If year is not NA, set Type to "First report"
CABI_present.loc[CABI_present["year"].notna(), "Type"] = "First report"

# Get earliest reference year
CABI_present["Reference Years"] = CABI_present["Reference"].apply(lambda x: re.findall(r"\(([0-9]{4})[a-e]?\)", x) if type(x) == str else [])

# Get earliest reference year
CABI_present["Earliest Reference"] = CABI_present["Reference Years"].apply(lambda x: min(x) if len(x) > 0 else None)

# If year is NA, set year to earliest reference year and type to "First reference"

CABI_present.loc[(CABI_present["year"].isna()) & (CABI_present["Earliest Reference"].notna()), "Type"] = "First reference"
CABI_present.loc[CABI_present["year"].isna(), "year"] = CABI_present.loc[CABI_present["year"].isna(), "Earliest Reference"]

# Create a True/False Native column
# True if Origin contains "Native"
# False if Origin contains "Introduced"
# Otherwise, NA

CABI_present.loc[CABI_present["Origin"].str.contains("Native", na=False), "Native"] = True
CABI_present.loc[CABI_present["Origin"].str.contains("Introduced", na=False), "Native"] = False

# Rename 'location'
CABI_present.rename(columns={"Continent/Country/Region": "location"}, inplace=True)

# Drop the sub-national locations
CABI_present = CABI_present.loc[
    ~CABI_present["location"].str.match(r"^-.*")
].reset_index(drop=True)

# Standardize columns
CABI_present = CABI_present[
    ["usageKey", "location", "year", "Type", "Reference", "Native"]
    ]

# Add a source column
CABI_present["Source"] = "CABI"

### GBIF data

print("Reading and cleaning GBIF data...")

# Already in mostly standard format: Read file and set column names
GBIF_occur = pd.read_csv(data_dir + "GBIF data/GBIF_first_records.csv",
                         # usageKey column as string
                        dtype={"usageKey": "str"},)
GBIF_occur.rename(columns={"country": "location", "years": "year", "species":"usageKey"}, inplace=True)

# Add a type column
GBIF_occur["Type"] = "First report"

# Add a reference column
GBIF_occur["Reference"] = "Counts API"

# Add a source column
GBIF_occur["Source"] = "GBIF"

### DAISIE data

print("Reading and cleaning DAISIE data...")

# Already mostly in standard format: Read file and set column names
# DAISIE first records dataset
DAISIE_occur = pd.read_csv(data_dir + "DAISIE data/DAISIE_distribution.csv", dtype={"usageKey ": "str"})

# Columns to keep from the DAISIE_occur data
# code_region
# region_country as location
# DAISIE_idspecies as codeDAISIE
# source as Reference
DAISIE_occur = DAISIE_occur[["DAISIE_idspecies", "start_year", "end_year", "code_region", "region_country", "source"]]
DAISIE_occur.rename(columns={"region_country": "location", "DAISIE_idspecies":"codeDAISIE", "source":"Reference"}, inplace=True)

# codeDAISIE as integer to merge with the link file
DAISIE_occur["codeDAISIE"] = DAISIE_occur["codeDAISIE"].astype(int)

# If there's a value for start_year, set year to start_year. If start_year is NA, set year to end_year
DAISIE_occur["text_year"] = DAISIE_occur["start_year"]
DAISIE_occur.loc[DAISIE_occur["text_year"].isna(), "text_year"] = DAISIE_occur.loc[DAISIE_occur["text_year"].isna(), "end_year"]

# Type == "First report"
# Source == "DAISIE"
# Native == False
DAISIE_occur["Type"] = "First report"
DAISIE_occur["Source"] = "DAISIE"
DAISIE_occur["Native"] = False

# DAISIE years contain a mix of values - some are single years, some are ranges
# Some include descriptions like "before 2000" or "probabbly around 1960 by symptoms"
# Some are missing values (?, 0, .)
# Clean year column
DAISIE_occur["year"] = DAISIE_occur["text_year"].apply(clean_DAISIE_year)

# Drop start_year, end_year, and text_year columns
DAISIE_occur.drop(columns=["start_year", "end_year", "text_year"], inplace=True)

# If year is None or NA, set Type to "First Reference"
DAISIE_occur.loc[DAISIE_occur["year"].isna(), "Type"] = "First Reference"

# Then apply clean_DAISIE_year to the Reference column
DAISIE_occur.loc[DAISIE_occur["year"].isna(), "year"] = DAISIE_occur.loc[DAISIE_occur["year"].isna(), "Reference"].apply(clean_DAISIE_year)

# If the year is still NA, set Type to "Not dated" and year to 2019 (DAISIE's last updated date)
DAISIE_occur.loc[DAISIE_occur["year"].isna(), "Type"] = "Not dated"
DAISIE_occur.loc[DAISIE_occur["year"].isna(), "year"] = 2019


# DAISIE link

DAISIE_link = pd.read_csv(data_dir + "link files/DAISIE_link.csv", dtype={"usageKey": "str"}, usecols=["usageKey", "codeDAISIE"])
# codeDAISIE as integer
DAISIE_link["codeDAISIE"] = DAISIE_link["codeDAISIE"].astype(int)

# Merge DAISIE_occur with DAISIE_link
DAISIE_merged = pd.merge(left=DAISIE_occur, right=DAISIE_link, how="left", on="codeDAISIE")

# Drop the column codeDAISIE
DAISIE_merged.drop(columns=["codeDAISIE"], inplace=True)

#### ASFR data

print("Reading and cleaning ASFR data...")

# Read file and set column names
ASFR_occur = pd.read_csv(
    data_dir + "species lists/by_database/AlienSpeciesFirstRecord.csv",
    usecols=["TaxonName", "Region", "FirstRecord", "Source"],
)

ASFR_occur.rename(
    columns={"TaxonName": "species", "Region": "location", "FirstRecord": "year", "Source": "Reference"},
    inplace=True,
)

# Add GBIF usageKey to ASFR species
ASFR_link = pd.read_csv(
    data_dir + "link files/ASFR_link.csv",
        # usageKey column as string
        dtype={"usageKey": "str"}
        ).rename(columns={"speciesASFR":"species"})

ASFR_merged = pd.merge(left=ASFR_occur, right=ASFR_link, how="left", on="species")

ASFR_merged = ASFR_merged.loc[
    ASFR_merged["usageKey"].notna()
]

# Drop species column
ASFR_merged.drop(columns=["species"], inplace=True)

# Add a type column
ASFR_merged["Type"] = "First report"

# Add a source column
ASFR_merged["Source"] = "ASFR"

# Add native column
ASFR_merged["Native"] = False

#### EPPO data
## EPPO reporting

print("Reading and cleaning EPPO reporting data...")

EPPO_occur = pd.read_csv(
    data_dir + "EPPO data/EPPO_first_reports.csv",
    # usageKey column as string
    dtype={"usageKey": "str"},
)

# Add Type column
EPPO_occur["Type"] = "First report"

# Create reference column: Num. Title links

EPPO_occur["Reference"] = EPPO_occur.apply(lambda x: f"{x['Num.']}. {x['Title']} {x['links']}", axis=1)

# Keep the standard columns
EPPO_countries = EPPO_occur[["usageKey", "location", "year", "Type", "Reference", "ISO3"]]

EPPO_countries["Source"] = "EPPO Reporting"

# All EPPO Reports are not native

EPPO_countries["Native"] = False

## EPPO distribution

print("Reading and cleaning EPPO distribution data...")

EPPO_dist = pd.read_csv(
    data_dir + "EPPO data/EPPO_distribution.csv",
    # usageKey column as string
    dtype={"usageKey": "str"},
)

# Remove ISO3 = NA (sub-national)

EPPO_dist = EPPO_dist.loc[EPPO_dist["ISO3"].notna()]

# Remove Status contains "Absent"
EPPO_dist = EPPO_dist.loc[~EPPO_dist["Status"].str.contains("Absent")]

# Set standard column names
EPPO_dist = EPPO_dist.rename(columns={"Country":"location", "First date type":"Type", "References": "Reference"})

# Create year column

# If "Type" is "First report", use "First date"
EPPO_dist.loc[EPPO_dist["Type"] == "First report", "year"] = EPPO_dist.loc[EPPO_dist["Type"] == "First report", "First date"]

# If "Type" is "First year listed", use min of "First date" and "First reference"
EPPO_dist.loc[EPPO_dist["Type"] == "First year listed", "year"] = EPPO_dist.loc[EPPO_dist["Type"] == "First year listed"].apply(lambda x: min(x["First date"], x["First reference"]), axis=1)

# If "Type" is "First year listed" and "First reference" is earlier than "First date", set "Type" to "First reference"
EPPO_dist.loc[(EPPO_dist["Type"] == "First year listed") & (EPPO_dist["year"] == EPPO_dist["First reference"]), "Type"] = "First reference"

# Set the same columns as EPPO reporting

EPPO_dist = EPPO_dist[["usageKey", "location", "year", "Type", "Reference", "ISO3"]].reset_index(drop=True)
EPPO_dist["Source"] = "EPPO Distribution"

# If Type is First report, set Native to False (otherwise leave as NA)
EPPO_dist.loc[EPPO_dist["Type"] == "First report", "Native"] = False

# Concat EPPO distribution with EPPO countries

EPPO_countries = pd.concat([EPPO_countries, EPPO_dist]).reset_index(drop=True)

#### Native range data

native_ranges = pd.read_csv(data_dir + "native ranges/all_sources_native_ranges.csv",
                # usageKey column as string
                dtype={"usageKey": "str"},)


# location
# First set as "DAISIE_region" (later update)
native_ranges["location"] = native_ranges["DAISIE_region"]

# Reference

native_ranges.rename(columns={"source": "Reference"}, inplace=True)
native_ranges.rename(columns={"usagekey": "usageKey"}, inplace=True)

# source
# If Reference starts with "DAISIE ", source = "DAISIE" and strip "DAISIE " from Reference

native_ranges.loc[
    native_ranges.Reference.str.startswith("DAISIE ", na=False), "Source"
] = "DAISIE"

native_ranges.loc[
    native_ranges.Reference.str.startswith("DAISIE ", na=False), "Reference"
] = native_ranges.loc[
    native_ranges.Reference.str.startswith("DAISIE ", na=False), "Reference"
].str.lstrip("DAISIE ")

# References that contain Takeuchi et al. 2017 are source NCSU, CIPM
native_ranges.loc[
    native_ranges.Reference.str.contains("Takeuchi et al. 2017", na=False), "Source"
] = "NCSU, CIPM"

# Remaining sources that are NA are source "Original"

native_ranges.loc[native_ranges.Source.isna(), "Source"] = "Original"

# Native
# Set to True
native_ranges["Native"] = True

# Make one manual fix to a Reference with a broken link
chrome_link = native_ranges.loc[native_ranges['Reference'].str.contains("chrome-extension://", na=False), "Reference"].values[0]
native_ranges.loc[native_ranges['Reference'] == chrome_link, "Reference"] = "https://catalog.extension.oregonstate.edu/sites/catalog/files/project/pdf/pnw648.pdf"

# Remove the residual index column
native_ranges.drop(columns=["Unnamed: 0"], inplace=True)

#### Country-name matching

print("Matching country names...")

countries_match = pd.read_csv(data_dir + "country files/country_codes.csv")

## GBIF - countries

GBIF_countries = pd.merge(
    left=GBIF_occur,
    right=countries_match,
    how="left",
    left_on="location",
    right_on="ISO2",
)

## ASFR - countries 

ASFR_countries = pd.merge(
    left=ASFR_merged,
    right=countries_match,
    how="left",
    left_on="location",
    right_on="NAME",
)

## CABI - countries

CABI_countries = pd.merge(
    left=CABI_present,
    right=countries_match,
    how="left",
    left_on="location",
    right_on="NAME",
)

## Native ranges - countries

native_ranges = pd.merge(
    left=native_ranges,
    right=countries_match,
    how="left",
    left_on="location",
    right_on="NAME",
)

## DAISIE mostly uses ISO3 but some are non-standard

# Match the countries
# Merge first based on ISO3
DAISIE_countries = pd.merge(
    left=DAISIE_merged,
    right=countries_match["ISO3"],
    how="left",
    left_on="code_region",
    right_on="ISO3",
)

# Keep the matches
DAISIE_countries_matched = DAISIE_countries.loc[DAISIE_countries["ISO3"].notna()]

# Then merge the unamtched based on NAME
DAISIE_countries_unmatched = DAISIE_countries.loc[DAISIE_countries["ISO3"].isna()]
DAISIE_countries_unmatched.drop(columns=["ISO3"], inplace=True)

DAISIE_countries_unmatched = pd.merge(
    left=DAISIE_countries_unmatched,
    right=countries_match[["NAME", "ISO3"]],
    how="left",
    left_on="location",
    right_on="NAME",
)

# Combine them back

DAISIE_countries = pd.concat([DAISIE_countries_matched, DAISIE_countries_unmatched]).reset_index(drop=True)

# Match unmatched countries using pycountry

print("Mapping CABI countries...")
CABI_countries = match_countries(CABI_countries)

print("Mapping GBIF countries...")
GBIF_countries = match_countries(GBIF_countries)

print("Mapping ASFR countries...")
ASFR_countries = match_countries(ASFR_countries)

print("Mapping DAISIE countries...")
DAISIE_countries = match_countries(DAISIE_countries)

print("Mapping native range countries...")
native_ranges = match_countries(native_ranges)

## Update native range locations
# If DAISIE_region is not na, location = bioregion - DAISIE_region
native_ranges.loc[~native_ranges.DAISIE_region.isna(), "location"] = native_ranges.loc[
    ~native_ranges.DAISIE_region.isna(), "bioregion"
] + " - " + native_ranges.loc[~native_ranges.DAISIE_region.isna(), "DAISIE_region"]

# Else, location = bioregion

native_ranges.loc[native_ranges.DAISIE_region.isna(), "location"] = native_ranges.loc[
    native_ranges.DAISIE_region.isna(), "bioregion"
]

## Remove un-needed columns:

CABI_countries.drop(columns=["ISO2", "NAME"], inplace=True)
GBIF_countries.drop(columns=["ISO2", "NAME"], inplace=True)
ASFR_countries.drop(columns=["ISO2","NAME"], inplace=True)
DAISIE_countries.drop(columns=["NAME", "code_region"], inplace=True)
native_ranges = native_ranges[["usageKey","ISO3","location", "Native", "Source", "Reference"]]

# Drop duplicates 

CABI_countries.drop_duplicates(inplace=True)
GBIF_countries.drop_duplicates(inplace=True)
ASFR_countries.drop_duplicates(inplace=True)
EPPO_countries.drop_duplicates(inplace=True)
DAISIE_countries.drop_duplicates(inplace=True)

## Add all five individual datasets with ISO3 matched to occurrences folder

print("Writing country-matches to csv...")

CABI_countries.to_csv(data_dir + "occurrences/CABI_first_records.csv", index=False)
GBIF_countries.to_csv(data_dir + "occurrences/GBIF_first_records.csv", index=False)
ASFR_countries.to_csv(data_dir + "occurrences/ASFR_first_records.csv", index=False)
EPPO_countries.to_csv(data_dir + "occurrences/EPPO_first_records.csv", index=False)
DAISIE_countries.to_csv(data_dir + "occurrences/DAISIE_first_records.csv", index=False)

native_ranges.to_csv(data_dir + "occurrences/native_ranges.csv", index=False)

# Combine all records

all_records = pd.concat(
    [CABI_countries, GBIF_countries, ASFR_countries, EPPO_countries, DAISIE_countries, native_ranges]
)

# Harmonize data types in all_records
# year should be a float
# usageKey should be a string
# for any usageKeys with ".0", remove ".0"

all_records["year"] = all_records["year"].astype(float)
all_records["usageKey"] = all_records["usageKey"].astype(str)
all_records["usageKey"] = all_records["usageKey"].str.replace("\.0","", regex=True)

# Write to csv

all_records.to_csv(data_dir + "occurrences/all_records.csv", index=False)

print("All records and individual source first records saved to .csv")

# If a record is the native range, it should be the earliest
# Set year temporarily to -99999

all_records.loc[all_records["Native"] == True, "year"] = -99999

# Get a dataset of the earliest record by species-country

first_records = (
    all_records[["usageKey", "ISO3", "year"]]
    .groupby(by=["usageKey", "ISO3"], as_index=False)
    .min()
)

print("Consolidating first records and their references...")

# Keep the reference, source, and native columns 

# If there are multiple records for the earliest species-country-year, 
# concatenate their references and sources

# This is a clunky way to do this... it takes 3 - 5 minutes.

first_records["Combo_ID"] = first_records.apply(lambda x: f"{x['usageKey']}_{x['ISO3']}_{x['year']}", axis=1)
all_records["Combo_ID"] = all_records.apply(lambda x: f"{x['usageKey']}_{x['ISO3']}_{x['year']}", axis=1)

# Very slow so we only want to do it for the multi-record combos

# Filter all_records to Combo_IDs in first_records with value_counts = 1
all_records_firsts = all_records.loc[all_records["Combo_ID"].isin(first_records["Combo_ID"])]
multi_record_IDs = all_records_firsts.groupby("Combo_ID").filter(lambda x: len(x) > 1).Combo_ID.unique()
single_record_IDs = all_records_firsts.loc[~all_records_firsts["Combo_ID"].isin(multi_record_IDs)].Combo_ID.unique()

# Craft the values for the multi-record combos
sources = []
references = []
native = []
record_type = []

for i, combo_id in enumerate(multi_record_IDs):
    # Filter to that combo
    subset = all_records.loc[all_records["Combo_ID"] == combo_id]

    # Concatenate sources and references
    sources += [", ".join(subset["Source"].unique())]
    try:
        references += [", ".join(subset["Reference"].unique())]
    except TypeError:
        references += [""]

    # If any Native columns are True, set Native to True. If some are False, set to False. If all are NA, set to NA.
    if subset["Native"].any():
        native += [True]
    elif subset["Native"].isna().all():
        native += [np.nan]
    else:
        native += [False]

    # If any of Type column are "First report" set Type to "First report", otherwise set to the first value
    if "First report" in subset["Type"].unique():
        record_type = "First report"
    else:
        record_type = subset["Type"].unique()[0]

    # Print out every 100 rows so I know how it's going...
    if i % 1000 == 0:
        print(f"Done with {i} of {len(multi_record_IDs)} rows... {round(i/len(multi_record_IDs),2)*100}%")

print("Done with multi-record combos!")

# Save them as a dataframe
multi_record_df = pd.DataFrame(
    {"Combo_ID": multi_record_IDs, "Source": sources, "Reference": references, "Native": native, "Type": record_type}
)

# Keep the single records as they are
single_record_df = all_records.loc[all_records["Combo_ID"].isin(single_record_IDs), ["Combo_ID", "Source", "Reference", "Native", "Type"]]

# Combine and merge
all_references = pd.concat([multi_record_df, single_record_df]).reset_index(drop=True)
first_records = first_records.merge(all_references, on="Combo_ID", how="left")

# Drop the combo ID column
first_records.drop(columns=["Combo_ID"], inplace=True)

# Set any years that are -99999 to NA
first_records.loc[first_records["year"] == -99999, "year"] = np.nan

# Write to csv
first_records.to_csv(data_dir + "occurrences/first_records.csv", index=False)

print(f"{len(first_records.index)} first records saved to .csv!")