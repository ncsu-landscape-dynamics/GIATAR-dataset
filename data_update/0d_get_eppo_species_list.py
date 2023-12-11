"""
File: data_update/0d_get_eppo_species_list.py
Author: Ariel Saffer
Date created: 2023-04-14
Description: Combine the EPPO species lists from the EPPO-main files
"""

import pandas as pd

import os
import dotenv

from datetime import date

dotenv.load_dotenv(".env")
data_dir = os.getenv("DATA_PATH")

# Get today's date
today = date.today()

### Get current EPPO list

### Download Bayer flat file

# Downloaded from EPPO data services dashboard, : https://data.eppo.int/user/
# Bayer flat file: https://data.eppo.int/documentation/bayer
# PFLNAME.TXT                plants
# GAINAME.TXT                animals
# GAFNAME.TXT                micro-organisms, viruses, abiotic growth factors

# Save all files to species lists/by_database/EPPO-main/

eppo = pd.concat(
    [
        pd.read_csv(data_dir + "species lists/by_database/EPPO-main/pflname.txt"),
        pd.read_csv(data_dir + "species lists/by_database/EPPO-main/gainame.txt"),
        pd.read_csv(data_dir + "species lists/by_database/EPPO-main/gafname.txt"),
    ],
    ignore_index=True,
)

# Reducing the data to one record (preferred name) per EPPO code

eppo_list = (
    eppo.loc[eppo["preferred"] == 1][["code", "fullname"]]
    .reset_index()
    .drop(columns=["index"])
)

eppo_list['New'] = True
eppo_list["Date"] = f"{today.year}-{today.month:02d}-{today.day:02d}"

# Compare
try:
    prev_eppo_list = pd.read_csv(data_dir + "species lists/by_database/eppo_full_list.csv")
    prev_eppo_list['New'] = False

    eppo_list = pd.concat([prev_eppo_list,eppo_list]).drop_duplicates(subset=['code'], keep="first")
except FileNotFoundError:
    pass

print("How many new (TRUE) and old (FALSE) records?")
print(eppo_list['New'].value_counts())

# Export consolidated list

eppo_list.to_csv(data_dir + "species lists/by_database/eppo_full_list.csv", index=False)