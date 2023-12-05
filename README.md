# GIATAR-database
Global Invasive and Alien Traits and Records Database


## Intro paragraphs


## Setting up the environment/installing

We use conda for environment management - 
```conda env create -n GIATAR_database -f environment.yml```

## Folders and files 

This repository contains the following code:

- data_query: change this one!
- data_update: scripts to obtain and update the database from its original sources

### Using the database - query functions

### Data update

All the scripts to obtain (via API, direct download, or webscraping) and consolidate data from the sources used are provided in the data_update folder. These scripts should be run sequentially (`0a_create_env.py`, `0b_get_asfr_species_list.py`, ..., `5_eppo_api_update.py`) to create the database or update the database with new data from each source. All scripts can be run sequentially using `run_data_update.py`, but due to changes in original source formatting that occur over time, we recommend running each script individually to ensure that it produces expected results.

`1a2_integrate_unmatched.py` is an optional script to resolve additional unmatched species using fuzzy-matching. Before running this script, run the R-script `R_script_here.R`.

### Paper and citation
