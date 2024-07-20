# GIATAR-dataset
Global Invasive and Alien Traits and Records Dataset


## About the dataset
Monitoring and managing the global spread of invasive and alien species requires accurate spatiotemporal records of species presence and information about the biological characteristics of species of interest including life cycle information, biotic and abiotic constraints and pathways of spread. The Global Invasive and Alien Traits And Records (GIATAR) dataset provides consolidated dated records of invasive and alien presence at the country-scale combined with a suite of biological information about pests of interest in a standardized, machine-readable format. We provide dated presence records for 45,695 alien taxa in 249 countries constituting 551,918 country-taxon pairs, joined with additional biological information for thousands of taxa. GIATAR is designed to be quickly updateable with future data and easy to integrate into ongoing research on global patterns of alien species movement using scripts provided to query and analyze data. GIATAR provides crucial data needed for researchers and policymakers to compare global invasion trends across a wide range of taxa. 

## Setting up the environment/installing

We use conda for environment management - 
```conda env create -n GIATAR_dataset -f environment.yml```
  Any Python version after 3.7 should be sufficient, we suggest 3.10 because of changes to pandas in 3.11
## Folders and files 

This repository contains the following code:

- query_functions: a set of python scripts for the user to quickly query common data from the dataset. 
- data_update: scripts to obtain and update the dataset from its original sources

### Using the dataset - query functions

Because GIATAR contains many different fields from different sources, we have provided some query functions to simplify using the dataset for the most common kinds of requests including querying records, biological traits, native ranges and common names. All scripts and functions are available in both Python and R.  A complete tutorial on the setup and usage of the query functions is available in the `query_functions` folder. 

### Data update

All the scripts to obtain (via API, direct download, or webscraping) and consolidate data from the sources used are provided in the `data_update` folder. These scripts should be run sequentially (`0a_create_env.py`, `0b_get_asfr_species_list.py`, ..., `5_eppo_api_update.py`) to create the dataset or update the dataset with new data from each source. All scripts can be run sequentially in with guiding instructions via `GIATAR_data_update.ipynb`. We recommend running each script individually to ensure that it produces the expected results, as there may be errors due to changes in original source formatting that occur over time. Please contact us if you run into issues!


### Paper and citation

_In revision_
