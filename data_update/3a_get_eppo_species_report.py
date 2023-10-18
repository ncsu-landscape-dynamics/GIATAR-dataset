"""
File: data_update/3a_get_eppo_species_report.py
Author: Ariel Saffer
Date created: 2023-04-14
Description: Scrape EPPO reports for all species in the invasive database
"""

import sys
import os
import dotenv
import pandas as pd
import numpy as np
from datetime import date
import spacy

sys.path.append(os.getcwd())

from data_update.data_functions import (
    country_from_eppo_reports,
    scrape_eppo_reports_species,
    get_record,
    country_from_eppo_reports,
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
    prev_section_table = pd.read_csv(f"{data_dir}/EPPO data/EPPO_reporting.csv")
    print("Imported EPPO reporting data.")
except FileNotFoundError:
    print("No previous EPPO reporting data found.")
    prev_section_table = pd.DataFrame()

# Read in previous data
try:
    prev_first_reports = pd.read_csv(f"{data_dir}/EPPO data/EPPO_first_reports.csv")
    print("Imported previous EPPO first reports.")
except FileNotFoundError:
    print("No previous EPPO first reports found.")
    prev_first_reports = pd.DataFrame()


# Applying the function to all EPPO codes in the invasive dataset

print(f"Getting EPPO reports for {len(codes)} species...")

read_tables = []

for i, code in enumerate(codes):
    table = scrape_eppo_reports_species(code)

    if i % 100 == 0:
        print(f"{i} out of {len(codes)} done!")

    if table is None:
        continue
    if table is np.nan:
        continue

    table["usageKey"] = eppo_invasive_new.loc[
        eppo_invasive_new["codeEPPO"] == code
    ].usageKey.values[0]

    read_tables.append(table)

# Create combined table and write to .csv

section_table = read_tables[0]

for table in range(1, len(read_tables)):
    section_table = pd.concat([section_table, read_tables[table]])

section_table = section_table.reset_index(drop=True)

# Append to previous table
combined_table = pd.concat([prev_section_table, section_table]).reset_index(drop=True)

# Export to csv
combined_table.to_csv(f"{data_dir}/EPPO data/EPPO_reporting.csv", index=False)
print(
    f"File complete! Added Species: {len(section_table.codeEPPO.unique())}, Rows: {len(section_table.index)}"
)

# Extract date of first record and place name from titles
# Select only first records

spacy.cli.download("en_core_web_sm")
print("Processing records from text...")

section_table["is_record"] = section_table.apply(lambda x: get_record(x.Title), axis=1)
section_table = section_table.loc[section_table["is_record"] == True].reset_index(
    drop=True
)

section_table = country_from_eppo_reports(section_table)

# Append to previous table

combined_first_reports = pd.concat([prev_first_reports, section_table]).reset_index(drop=True)

combined_first_reports.to_csv(f"{data_dir}/EPPO data/EPPO_first_reports.csv", index=False)

print(f"{len(section_table.index)} new records added and saved.")