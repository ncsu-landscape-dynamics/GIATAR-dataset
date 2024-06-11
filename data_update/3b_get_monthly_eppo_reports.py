"""
File: data_update/3b_get_monthly_eppo_reports.py
Author: Ariel Saffer
Date created: 2023-04-14
Description: Get the previous month's "first records" published via the EPPO reporting service (run each month)
"""

import os
import dotenv
import sys
import pandas as pd
from datetime import date
import spacy

spacy.cli.download("en_core_web_sm")

sys.path.append(os.getcwd())

dotenv.load_dotenv(".env")
data_dir = os.getenv("DATA_PATH")
last_update = os.getenv("EPPO_REP_UPDATED")

from data_update.data_functions import (
    get_record,
    scrape_monthly_eppo_report,
    get_species,
    country_from_eppo_reports,
)

# URL is: f"https://gd.eppo.int/reporting/Rse-{year}-{month}"

# Get current year and month

today = date.today()
year = today.year
month = today.month

# Get last updated year and month

last_update = last_update.split("-")
last_year = int(last_update[0])
last_month = int(last_update[1])

# Create a list of years and months between last update and now

# Remaining months of last year
months = [f"{i:02d}" for i in range(last_month + 1, 13)]
years = [last_year] * len(months)

# All months of years in between last year and this year
for i in range(last_year + 1, year):
    months += [f"{i:02d}" for i in range(1, 13)]
    years += [i] * 12

# Past months of this year (up to but not including this month)

months += [f"{i:02d}" for i in range(1, today.month)]
years += [year] * len([f"{i:02d}" for i in range(1, today.month)])

# For species matching

eppo_gbif = pd.read_csv(data_dir + "species lists/gbif_matched/eppo_gbif.csv", dtype={"usageKey":str})
species_cols = pd.read_csv(data_dir + "EPPO data/eppo_full_list.csv")
species_cols["fullname"] = species_cols.apply(lambda x: x.fullname.lower(), axis=1)

# Loop through each month and extract the data

section_tables = pd.DataFrame()

for year, month in zip(years, months):

    print(f"Getting reports for {year}-{month}...")

    # Save the extracted data to .csv
    section_table = scrape_monthly_eppo_report(year, month)
    section_table.to_csv(
        data_dir + f"/EPPO data/monthly_reports/reporting_{year}-{month}.csv", index=False
    )

    if len(section_table.index) != 0:

        # Get just the "first reports of/new finding of"

        section_table["is_record"] = section_table.apply(lambda x: get_record(x.Title), axis=1)
        section_table = section_table.loc[section_table["is_record"] == True].reset_index(
            drop=True
        )

        if len(section_table.index) != 0:

            # Extract place names (NER) - match to ISO3 codes

            section_table = country_from_eppo_reports(section_table)

            # Next, extract species, match EPPO code and then GBIF usageCode
            # Species name
            section_table["origTaxon"] = section_table.apply(
                lambda x: get_species(x["Title"]), axis=1
            )

            section_table = section_table.merge(
                species_cols, how="left", right_on="fullname", left_on="origTaxon"
            )
            section_table.rename(
                columns={"code": "codeEPPO", "origTaxon": "taxonEPPO"}, inplace=True
            )
            section_table.drop(columns="fullname", inplace=True)

            # Bring in GBIF usageKey
            section_table = section_table.merge(
                eppo_gbif[["codeEPPO", "usageKey"]], how="left", on="codeEPPO"
            )

            # Write out monthly reports to csv
            section_table.to_csv(
                data_dir + f"/EPPO data/monthly_reports/new_records_{year}-{month}.csv", index=False
            )

            # Append to section_tables
            section_tables = pd.concat([section_tables, section_table])

# If there are unmatched species, this could include records with no codes.
# E.g. not found in GBIF. We may want to check and design a process for those at some point.

# Append to exisiting records
eppo_first_records = pd.read_csv(f"{data_dir}/EPPO data/EPPO_first_reports.csv", dtype={"usageKey":str})
section_tables = pd.concat(
    [eppo_first_records, section_tables.drop(columns="taxonEPPO")]
).drop_duplicates()

section_tables.to_csv(f"{data_dir}/EPPO data/EPPO_first_reports.csv", index=False)

os.environ["EPPO_REP_UPDATED"]=f"'{today.year}-{today.month:02d}-{today.day:02d}\n'"
dotenv.set_key('.env', "key", os.environ["EPPO_REP_UPDATED"])