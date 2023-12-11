# GIATAR-database
Global Invasive and Alien Traits and Records Database


## Intro paragraphs


## Setting up the environment/installing

We use conda for environment management - 
```conda env create -n GITAR_database -f environment.yml```

## Folders and files 

This repository contains the following code:

- data_query: change this one!
- data_update: scripts to obtain and update the database from its original sources

### Using the database - query functions

### Data update

All the scripts to obtain (via API, direct download, or webscraping) and consolidate data from the sources used are provided in the data_update folder. These scripts should be run sequentially (`0a_create_env.py`, `0b_get_asfr_species_list.py`, ..., `5_eppo_api_update.py`) to create the database or update the database with new data from each source. All scripts can be run sequentially in with guiding instructions via `GIATAR_data_update.ipynb`. We recommend running each script individually to ensure that it produces the expected results, as there may be errors due to changes in original source formatting that occur over time. Please contact us if you run into issues!


### Paper and citation
