"""
File: data_update/3c_get_eppo_species_dist.py
Author: Ariel Saffer
Date created: 2023-04-14
Description: Scrape EPPO distribution data for all species in the invasive database
"""

import sys
import os
import dotenv
import pandas as pd
import numpy as np
from datetime import date

sys.path.append(os.getcwd())

from data_update.data_functions import (
    scrape_eppo_distribution_species,
)

# Get data dir - invasive database folder

dotenv.load_dotenv(".env")
data_dir = os.getenv("DATA_PATH")

# Get today's date
today = date.today()

# Bring in EPPO invasive records

eppo_link = pd.read_csv(data_dir + "link files/EPPO_link.csv")
eppo_new = pd.read_csv(data_dir + "species lists/new/eppo_new.csv")
eppo_invasive_new = eppo_link.loc[eppo_link.usageKey.isin(eppo_new.usageKey)].reset_index(drop=True)
codes = eppo_invasive_new["codeEPPO"].unique()

# Read in previous data
try:
    prev_section_table = pd.read_csv(f"{data_dir}/EPPO data/EPPO_distribution.csv")
    print("Imported EPPO distribution data.")
except FileNotFoundError:
    print("No previous EPPO distribution data found.")
    prev_section_table = pd.DataFrame()

# Applying the function to all EPPO codes in the invasive dataset

print(f"Getting EPPO distribution data for {len(codes)} species...")

read_tables = []

for i, code in enumerate(codes):
    table = scrape_eppo_distribution_species(code)

    if i % 100 == 0:
        print(f"{i} out of {len(codes)} done!")

    if table is None:
        continue
    if table is np.nan:
        continue

    table["usageKey"] = eppo_link.loc[
        eppo_link["codeEPPO"] == code
    ].usageKey.values[0]

    read_tables.append(table)

# Create combined table, clean, and write to .csv

section_table = read_tables[0]

for table in range(1, len(read_tables)):
    section_table = pd.concat([section_table, read_tables[table]])

section_table = section_table.reset_index(drop=True)

# Map ISO2 to ISO3

countries = pd.read_csv(data_dir + "country files/country_codes.csv", usecols=["ISO2", "ISO3"])

section_table = pd.merge(
        left=section_table,
        right=countries,
        how="left",
        left_on="ISO2",
        right_on="ISO2",
    )

# Add today's date

section_table["Date"] = f"{today.year}-{today.month:02d}-{today.day:02d}"

# Append to previous table
combined_table = pd.concat([prev_section_table, section_table]).drop_duplicates(
    subset=section_table.columns.difference(["Date"])
    ).reset_index(drop=True).reset_index(drop=True)

# Export to csv
combined_table.to_csv(f"{data_dir}/EPPO data/EPPO_distribution.csv", index=False)
print(
    f"File complete! Added Species: {len(section_table.codeEPPO.unique())}, Rows: {len(section_table.index)}"
)