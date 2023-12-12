# GIATAR-database
Global Invasive and Alien Traits and Records Database


## About the database
The global spread of alien and invasive species is a multi-billion dollar-a-year problem and a major threat to the stability of the world's ecosystems. In order to predict and prevent future spread, dated occurence records are a useful tool for scientists to understand the dynamics effecting the spread of different organisms.  The GIATAR database contains the earliest dated occurrence records consolidated from the CABI Invasive Species Compendium (CABI, 2021), the European and Mediterranean Plant Protection Organization Global Database (EPPO, 2022), the Delivering Alien Invasive Species Inventories for Europe (DAISIE) dataset (Hulme et al., 2010), the Alien Species First Records dataset (ASFR; Seebens, 2023; Seebens et al., 2017b) and the Global Biodiversity Information Facility occurrence data (GBIF, n.d.) for a total of 31,084 invasive taxa, constituting 551,918 country-taxon pairs. 

## Setting up the environment/installing

We use conda for environment management - 
```conda env create -n GIATAR_database -f environment.yml```
  Any Python version after 3.7 should be sufficient, we suggest 3.10 because of changes to pandas in 3.11
## Folders and files 

This repository contains the following code:

- query_functions: a set of python scripts for the user to quickly query common data from the database. 
- data_update: scripts to obtain and update the database from its original sources

### Using the database - query functions

Because GIATAR contains many different fields from different sources, we have provided some query functions to simplify using the database for the most common kinds of requests including querying records, biological traits, native ranges and common names. All scripts and functions are in Python.  A complete tutorial on the setup and usage of the query functions is available in the `query_functions` folder. 

### Data update

All the scripts to obtain (via API, direct download, or webscraping) and consolidate data from the sources used are provided in the `data_update` folder. These scripts should be run sequentially (`0a_create_env.py`, `0b_get_asfr_species_list.py`, ..., `5_eppo_api_update.py`) to create the database or update the database with new data from each source. All scripts can be run sequentially in with guiding instructions via `GIATAR_data_update.ipynb`. We recommend running each script individually to ensure that it produces the expected results, as there may be errors due to changes in original source formatting that occur over time. Please contact us if you run into issues!


### Paper and citation

_Pending_
