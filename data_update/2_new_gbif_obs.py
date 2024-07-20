"""
File: data_update/2_new_gbif_obs.py
Author: Ariel Saffer
Date created: 2023-04-14
Description: Get the new GBIF observations for all species, only since most recent data queried
"""

import pandas as pd
import itertools
import sys
import os
import dotenv
from datetime import date

sys.path.append(os.getcwd())

from data_update.data_functions import write_gbif_counts, call_gbif_api

# Load variables and paths from .env

dotenv.load_dotenv(".env")
data_dir = os.getenv("DATA_PATH")
last_update = os.getenv("GBIF_OBS_UPDATED")
base_year = int(os.getenv("BASE_OBS_YEAR"))

# Get today's date
today = date.today()

# Start with master list

species_list = pd.read_csv(data_dir + "species lists/invasive_all_source.csv", dtype={"usageKey":str})

# Get keys that belong to new species (need full history)
new_keys = pd.read_csv(data_dir + "species lists/new/new_usageKeys.csv", dtype={"usageKey":str})

# Only keep usageKeys that don't start with X
new_keys = new_keys[~new_keys['usageKey'].str.startswith('X')]
species_list = species_list[~species_list['usageKey'].str.startswith('X')]

# Get list of species that are new
new_invasive_species = species_list.loc[species_list['usageKey'].isin(new_keys['usageKey']), "usageKey"].unique()
prev_species = species_list.loc[~species_list['usageKey'].isin(new_keys['usageKey']), "usageKey"].unique()

# Convert all usageKeys to integers
new_invasive_species = [int(float(x)) for x in new_invasive_species]
prev_species = [int(float(x)) for x in prev_species]

# For previous species, we only need new data
# Get years for which data is needed

current_year = date.today().year
try:
    last_update_year = int(last_update[0:4])
except: 
    # Default year to start if there is no prior data
    last_update_year = base_year

print(f"Getting data from year {last_update_year} to present ({current_year})"
      f" for {len(prev_species)} already-included species.")

if current_year == last_update_year:
    years = [current_year]
else:
    years = list(range(last_update_year, current_year + 1))


# Create the dataframe with species and years to request from API

species_year = [prev_species, years]
species_years = list(itertools.product(*species_year))

print(f"Getting data from year {base_year} to present ({current_year})"
      f" for {len(new_invasive_species)} new species.")

years = list(range(base_year, current_year + 1))
species_year = [new_invasive_species, years]
species_years += list(itertools.product(*species_year))

api_calls_df = pd.DataFrame(species_years, columns=["species", "years"])

# Writing out the API calls
api_calls_df["api_call"] = api_calls_df.apply(write_gbif_counts, axis=1)

# Send API calls

print(f"Prepared {len(api_calls_df.api_call)} API calls...")

results = []

for (
    i, call
) in enumerate(api_calls_df.api_call):
    try:
        result = call_gbif_api(call)
    except:
        print(f"Failed on API call {i}!")
        result = "Failed" # Placeholder for failed API calls 
    results.append(result)
    if i % 100 == 0:
        print(f"{i} out of {len(api_calls_df.api_call)} API calls done!")


# Reformat country/count data into dataframe rows per entry

api_calls_df["result"] = results
api_calls_df[["country", "counts"]] = api_calls_df.result.apply(pd.Series)

all_counts = (
    api_calls_df.drop(columns="result")
    .set_index(["species", "years", "api_call"])
    .apply(pd.Series.explode)
    .reset_index()
)

# Set species datatype to integer and then string

all_counts["species"] = all_counts["species"].astype(int).astype(str)

# Save full response file to CSV (just in case it breaks!)

print("Saving preliminary file...")

all_counts.to_csv(
    data_dir + "GBIF data/intermediate_files/new_GBIF_species_obs.csv", index=False
)
print(
    f"File complete! Species: {len(all_counts.species.unique())}, Rows: {len(all_counts.index)}"
)

# Remove NAs - year-species pairs that did not have any observations, save to CSV

print("Preparing clean data...")

clean_counts = all_counts.loc[all_counts["country"].notnull()]

clean_counts.to_csv(data_dir + "GBIF data/new_GBIF_species_obs_clean.csv", index=False)

print(
    f"File complete! Species: {len(clean_counts.species.unique())}, Rows: {len(clean_counts.index)}"
)


# Use the earliest observation years to generate first records

print("Preparing first records...")

first_records = (
    clean_counts[["species", "country", "years"]]
    .groupby(by=["species", "country"], as_index=False)
    .min()
)

# Save to CSV

first_records.to_csv(data_dir + "GBIF data/new_GBIF_first_records.csv", index=False)

# Consolidate with and replace previous first records

previous_first_records = pd.read_csv(data_dir + "GBIF data/GBIF_first_records.csv", dtype={"usageKey":str})

first_records = (
    pd.concat([previous_first_records, first_records])
    .reset_index(drop=True)
    .drop_duplicates()
)

# Set species datatype to integer and then string (again)

first_records["species"] = first_records["species"].astype(int).astype(str)

# Regroup by species and country to get the earliest first record

first_records = (
    first_records[["species", "country", "years"]]
    .groupby(by=["species", "country"], as_index=False)
    .min()
)

# Save to CSV

first_records.to_csv(data_dir + "GBIF data/GBIF_first_records.csv", index=False)

print(
    f'GBIF occurrence data included {len(clean_counts.index)} "first records" for {len(clean_counts.species.unique())} species.'
)

# Drop all rows and write new_keys back as a blank file

new_keys = new_keys.drop(new_keys.index)
new_keys.to_csv(data_dir + "species lists/new/new_usageKeys.csv", index=False)

# Update the date in the .env file

os.environ["GBIF_OBS_UPDATED"]=f"{today.year}-{today.month:02d}-{today.day:02d}"
dotenv.set_key('.env', "GBIF_OBS_UPDATED", os.environ["GBIF_OBS_UPDATED"])