"""
File: query_functions/GIATAR_query_functions.py
Author: Thom Worm and Ariel Saffer
Date created: 2023-04-14
Description: Functions to facilitate querying the GIATAR database
"""

import pandas as pd
import numpy as np
import os
import pygbif
import dotenv
import requests
import zipfile
import io


#### DATA PATH ####
def create_dotenv(dp):
    """
    Creates a .env file in the current directory and writes the provided data path to it.

    Args:
        dp (str): The data path to be written to the .env file.

    Returns:
        None
    """
    # create .env file in current directory
    # write DATA_PATH to .env file
    with open(".env", "w") as f:
        f.write(f"DATA_PATH={dp}")


# if .env file exists, get DATA_PATH from .env file
if os.path.exists(".env"):
    data_path = dotenv.get_key(".env", "DATA_PATH")
    os.chdir(data_path)
else:
    print("No .env file found. Please use `create_dotenv()` to create a .env file")
    # prompt user for data path
    dp = input("Please enter the path to the data folder: ")
    create_dotenv(dp)


if "invasive_all_source" not in globals():
    global invasive_all_source
    invasive_all_source = pd.read_csv(
        r"species lists\invasive_all_source.csv", dtype={"usageKey": "str"}
    )


if "first_records" not in globals():
    global first_records
    first_records = pd.read_csv(
        r"occurrences\first_records.csv", dtype={"usageKey": "str"}, low_memory=False
    )


def get_species_name(usageKey):
    """
    Retrieve the species name corresponding to a given usage key.
    Args:
        usageKey (int or str): The usage key for which to retrieve the species name.
                               If the usage key is not a string, it will be converted
                               to a string and any ".0" will be removed.
    Returns:
        str: The canonical name of the species corresponding to the given usage key,
             if found in the invasive_all_source DataFrame.
    Raises:
        KeyError: If the usage key is not found in the invasive_all_source DataFrame.
    """
    # if usagekey is not a string, convert to string and remove ".0"
    if not isinstance(usageKey, str):
        usageKey = str(usageKey).replace(".0", "")

    if usageKey in invasive_all_source["usageKey"].values:
        return invasive_all_source.loc[
            invasive_all_source["usageKey"] == usageKey, "canonicalName"
        ].values[0]


def get_usageKey(species_name):
    """
    Retrieve the usage key for a given species name from various sources.
    This function checks multiple columns in the `invasive_all_source` DataFrame
    to find the usage key associated with the provided species name. If the species
    name is not found in the DataFrame, it attempts to retrieve the usage key from
    the GBIF database using the pygbif library.
    Parameters:
    species_name (str): The name of the species for which to retrieve the usage key.
    Returns:
    str: The usage key associated with the species name if found, otherwise None.
    Notes:
    - The function first checks if the `invasive_all_source` DataFrame is loaded,
      and if not, it loads the DataFrame from a CSV file.
    - The function checks the following columns in order: "canonicalName", "taxonSINAS",
      "taxonEPPO", "taxonCABI", "usageKey", "speciesGBIF", "taxonDAISIE".
    - If the species name is a digit or starts with "xx" or "XX", it is returned as is.
    - If the species name is not found in the DataFrame, the function attempts to
      retrieve the usage key from the GBIF database using the pygbif library.
    - If the species name is not found in both the DataFrame and the GBIF database,
      the function prints an error message and returns None.
    """
    if "invasive_all_source" not in globals():
        global invasive_all_source
        invasive_all_source = pd.read_csv(f"species lists\invasive_all_source.csv")

    if species_name in invasive_all_source["canonicalName"].values:
        return invasive_all_source.loc[
            invasive_all_source["canonicalName"] == species_name, "usageKey"
        ].values[0]
    elif species_name in invasive_all_source["taxonSINAS"].values:
        return invasive_all_source.loc[
            invasive_all_source["taxonSINAS"] == species_name, "usageKey"
        ].values[0]
    elif species_name in invasive_all_source["taxonEPPO"].values:
        return invasive_all_source.loc[
            invasive_all_source["taxonEPPO"] == species_name, "usageKey"
        ].values[0]
    elif species_name in invasive_all_source["taxonCABI"].values:
        return invasive_all_source.loc[
            invasive_all_source["taxonCABI"] == species_name, "usageKey"
        ].values[0]
    elif species_name in invasive_all_source["usageKey"].values:
        return species_name
    elif species_name in invasive_all_source["canonicalName"].values:
        return invasive_all_source.loc[
            invasive_all_source["speciesGBIF"] == species_name, "usageKey"
        ].values[0]
    elif species_name in invasive_all_source["taxonDAISIE"].values:
        return invasive_all_source.loc[
            invasive_all_source["taxonDAISIE"] == species_name, "usageKey"
        ].values[0]
    # elif species name is digits or starts with "xx" or "XX" return species name
    elif (
        species_name.isdigit()
        or species_name.startswith("xx")
        or species_name.startswith("XX")
    ):
        return species_name
    else:
        try:
            gbif = pygbif.species.name_backbone(name=species_name, rank="species")
            return str(gbif["usageKey"])
        except KeyError:
            print("species not found in Database or GBIF")
            return None


def get_all_species():
    """
    Retrieve a list of all species names from the invasive_all_source DataFrame.

    This function iterates through all rows in the invasive_all_source DataFrame and collects species names based on the following criteria:
    - If the 'rank' column value is 'SPECIES', 'FORM', 'SUBSPECIES', or 'VARIETY', the 'canonicalName' column value is added to the list.
    - If the 'rank' column value does not match the above criteria and 'taxonEPPO' is not null, the 'taxonEPPO' column value is added to the list.
    - If 'taxonEPPO' is null and 'taxonSINAS' is not null:
        - If the 'taxonSINAS' column value is already in the list, the 'taxonCABI' column value is added instead.
        - Otherwise, the 'taxonSINAS' column value is added to the list.
    - If 'taxonSINAS' is null and 'taxonCABI' is not null, the 'taxonCABI' column value is added to the list.

    Returns:
        list: A list of species names collected from the invasive_all_source DataFrame.
    """
    # iterate through all rows in invasive_all_source and return a list of all species names
    # if rank is 'SPECIES', 'FORM', 'SUBSPECIES', 'VARIETY' return canonicalName
    # otherwise if taxonSINAS or taxonCABI is not null return that
    # collect the result of all rows into a list and return that
    species_list = []
    for index, row in invasive_all_source.iterrows():
        if row["rank"] in ["SPECIES", "FORM", "SUBSPECIES", "VARIETY"]:
            species_list.append(row["canonicalName"])
        elif pd.notnull(row["taxonEPPO"]):
            species_list.append(row["taxonEPPO"])
        elif pd.notnull(row["taxonSINAS"]):
            # if row['taxonSINAS'] in species_list, append row['taxonCABI'] instead
            if row["taxonSINAS"] in species_list:
                species_list.append(row["taxonCABI"])
            else:
                species_list.append(row["taxonSINAS"])
        elif pd.notnull(row["taxonCABI"]):
            species_list.append(row["taxonCABI"])
    return species_list


def check_species_exists(species_name):
    """
    Check if a species exists in the database.

    This function takes a species name or usageKey and checks if it exists in the invasive_all_source database.

    Parameters:
    species_name (str): The name or usageKey of the species to check.

    Returns:
    bool: True if the species exists in the database, False otherwise.
    """
    # function takes a species name or usageKey and checks if it exists in the database
    if get_usageKey(species_name) in invasive_all_source["usageKey"].values:
        return True
    else:
        return False


def get_first_introductions(
    species_name,
    check_exists=False,
    ISO3_only=False,
    import_additional_native_info=True,
):
    """
    Retrieve the first introduction records for a given species.

    This function retrieves the first introduction records for a specified species from the GIATAR database.
    It can optionally check if the species exists in the database, filter the results to only include ISO3 codes,
    and import additional native range information.

    Args:
        species_name (str): The name of the species for which to retrieve the first introduction records.
        check_exists (bool, optional): If True, raises a KeyError if the species is not in the database. Defaults to False.
        ISO3_only (bool, optional): If True, returns only records with 3-character ISO3 codes. Defaults to False.
        import_additional_native_info (bool, optional): If True, imports additional native range information. Defaults to True.

    Returns:
        pd.DataFrame: A DataFrame containing the first introduction records for the specified species.

    Raises:
        KeyError: If check_exists is True and the species is not found in the database.
    """
    usageKey = get_usageKey(species_name)
    if check_exists:
        if not check_species_exists(usageKey):
            raise KeyError(
                "Species not in Database. Try checking master list with get_all_species()"
            )

    # Create DataFrame of all first introductions where usageKey matches
    df = first_records.loc[first_records["usageKey"] == usageKey].copy()

    if ISO3_only:
        # Return DataFrame where ISO3 column is 3 characters long
        df = df.loc[df["ISO3"].str.len() == 3]

    if import_additional_native_info:
        native_ranges = get_native_ranges(usageKey, ISO3=df["ISO3"].unique().tolist())
        # Remove rows where Native is NaN
        native_ranges = native_ranges.loc[native_ranges["Native"].notna()]
        for index, row in native_ranges.iterrows():
            if row["Native"]:
                df.loc[df["ISO3"] == row["ISO3"], "Native"] = True
            else:
                df.loc[df["ISO3"] == row["ISO3"], "Native"] = False
        return df[~df["ISO3"].isin(["ZZ", "XL", "XZ"])]

    else:
        return df[~df["ISO3"].isin(["ZZ", "XL", "XZ"])]


def get_all_introductions(
    species_name, check_exists=False, ISO3_only=True, import_additional_native_info=True
):
    """
    Retrieve all introduction records for a given species.
    Parameters:
    species_name (str): The name of the species to query.
    check_exists (bool, optional): If True, check if the species exists in the database before querying. Defaults to False.
    ISO3_only (bool, optional): If True, filter results to only include records with valid ISO3 country codes. Defaults to True.
    import_additional_native_info (bool, optional): If True, import additional native range information. Defaults to True.
    Returns:
    pandas.DataFrame: A DataFrame containing introduction records for the specified species, optionally filtered by ISO3 codes and native range information.
    Raises:
    KeyError: If check_exists is True and the species does not exist in the database.
    """
    usageKey = get_usageKey(species_name)
    if check_exists == True:
        if not check_species_exists(usageKey):
            raise KeyError(
                "Species not in Database. Try checking master list with get_all_species()"
            )

    # if usagekey is a string only containing numbers
    if isinstance(usageKey, int):
        usageKey = str(usageKey)

    if not usageKey.isnumeric():
        "getting usageKey from species name"
        usageKey = get_usageKey(usageKey)

    # create df of all first introductinos where usageKey = usageKey
    if "all_records" not in globals():
        global all_records
    all_records = pd.read_csv(
        r"occurrences\all_records.csv", dtype={"usageKey": str}, low_memory=False
    )

    df = all_records.loc[all_records["usageKey"] == usageKey]
    # for each unique "ISO3" in df, get the first row were year is min
    # df = df.loc[df.groupby("ISO3")["year"].idxmin()]
    if ISO3_only == True:
        # return df where ISO3 column is 3 characters long
        df = df.loc[df["ISO3"].str.len() == 3]

    if import_additional_native_info == True:
        native_ranges = get_native_ranges(usageKey, ISO3=df["ISO3"].unique().tolist())
        # remove rows where Native is nan
        # for each row in native_ranges, if native is not nan, set native in df to Native
        native_ranges = native_ranges.loc[native_ranges["Native"].notna()]
        for index, row in native_ranges.iterrows():
            if row["Native"] == True:
                df.loc[df["ISO3"] == row["ISO3"], "Native"] = True
            else:
                df.loc[df["ISO3"] == row["ISO3"], "Native"] = False
        return df[~df["ISO3"].isin(["ZZ", "XL", "XZ"])]
    else:
        return df[~df["ISO3"].isin(["ZZ", "XL", "XZ"])]


def get_ecology(species_name, check_exists=False):
    """
    Retrieve ecological data for a given species from various CSV files.
    Args:
        species_name (str): The name of the species to retrieve data for.
        check_exists (bool, optional): If True, checks if the species exists in the database before proceeding. Defaults to False.
    Raises:
        KeyError: If check_exists is True and the species does not exist in the database.
    Returns:
        dict: A dictionary where keys are the names of the data sources and values are DataFrames containing the relevant data for the species.
    """
    if check_exists == True:
        if not check_species_exists(species_name):
            raise KeyError(
                "Species not in Database. Try checking master list with get_all_species()"
            )
    result_dict = {}

    usageKey = get_usageKey(species_name)

    # load csv for each of these files'CABI_rainfall', 'CABI_airtemp', 'CABI_climate', 'CABI_environments', 'CABI_lat_alt', 'CABI_water_tolerances'

    CABI_rainfall = pd.read_csv(
        r"CABI data\CABI_tables\torainfall.csv", dtype={"usageKey": "str"}
    )
    CABI_airtemp = pd.read_csv(
        r"CABI data\CABI_tables\toairTemperature.csv", dtype={"usageKey": "str"}
    )
    CABI_climate = pd.read_csv(
        r"CABI data\CABI_tables\toclimate.csv", dtype={"usageKey": "str"}
    )
    CABI_environments = pd.read_csv(
        r"CABI data\CABI_tables\toenvironments.csv", dtype={"usageKey": "str"}
    )
    CABI_lat_alt = pd.read_csv(
        r"CABI data\CABI_tables\tolatitudeAndAltitudeRanges.csv",
        dtype={"usageKey": "str"},
    )
    CABI_natural_enemies = pd.read_csv(
        r"CABI data\CABI_tables\tonaturalEnemies.csv", dtype={"usageKey": "str"}
    )
    CABI_water_tolerances = pd.read_csv(
        r"CABI data\CABI_tables\towaterTolerances.csv", dtype={"usageKey": "str"}
    )
    CABI_wood_packaging = pd.read_csv(
        r"CABI data\CABI_tables\towoodPackaging.csv", dtype={"usageKey": "str"}
    )
    DAISIE_habitats = pd.read_csv(
        r"DAISIE data\DAISIE_habitat.csv", dtype={"usageKey": "str"}
    )
    CABI_plant_trade = pd.read_csv(
        r"CABI data\CABI_tables\toplantTrade.csv", dtype={"usageKey": "str"}
    )
    # return a list of all rows where usageKey = usageKey
    # place rows into a dataframe and put into results_dict with key = filename
    result_dict["CABI_rainfall"] = CABI_rainfall.loc[
        CABI_rainfall["usageKey"] == usageKey
    ]
    result_dict["CABI_airtemp"] = CABI_airtemp.loc[CABI_airtemp["usageKey"] == usageKey]
    result_dict["CABI_climate"] = CABI_climate.loc[CABI_climate["usageKey"] == usageKey]
    result_dict["CABI_environments"] = CABI_environments.loc[
        CABI_environments["usageKey"] == usageKey
    ]
    result_dict["CABI_lat_alt"] = CABI_lat_alt.loc[CABI_lat_alt["usageKey"] == usageKey]
    result_dict["CABI_water_tolerances"] = CABI_water_tolerances.loc[
        CABI_water_tolerances["usageKey"] == usageKey
    ]
    result_dict["CABI_wood_packaging"] = CABI_wood_packaging.loc[
        CABI_wood_packaging["usageKey"] == usageKey
    ]
    result_dict["DAISIE_habitats"] = DAISIE_habitats.loc[
        DAISIE_habitats["usageKey"] == usageKey
    ]
    result_dict["CABI_natural_enemies"] = CABI_natural_enemies.loc[
        CABI_natural_enemies["usageKey"] == usageKey
    ]
    result_dict["CABI_plant_trade"] = CABI_plant_trade.loc[
        CABI_plant_trade["usageKey"] == usageKey
    ]
    # remove empty keys in result_dict
    result_dict = {k: v for k, v in result_dict.items() if not v.empty}

    return result_dict


def get_hosts_and_vectors(species_name, check_exists=False):
    """
    Retrieve host and vector information for a given species from various data sources.
    This function queries multiple CSV files to gather information about hosts and vectors
    associated with a specified species. The results are returned as a dictionary of DataFrames.
    Parameters:
    species_name (str): The name of the species to query.
    check_exists (bool): If True, checks if the species exists in the database before querying.
                         Raises a KeyError if the species does not exist. Default is False.
    Returns:
    dict: A dictionary where keys are the names of the data sources and values are DataFrames
          containing the query results. Empty DataFrames are excluded from the dictionary.
    Raises:
    KeyError: If check_exists is True and the species does not exist in the database.
    Example:
    >>> results = get_hosts_and_vectors("species_name", check_exists=True)
    >>> print(results.keys())
    dict_keys(['CABI_tohostPlants', 'CABI_topathwayVectors', 'CABI_tovectorsAndIntermediateHosts', 'EPPO_hosts', 'DAISIE_pathways', 'DAISIE_vectors', 'CABI_topathwayCauses'])
    """
    os.chdir(data_path)
    if check_exists == True:
        if not check_species_exists(species_name):
            raise KeyError(
                "Species not in Database. Try checking master list with get_all_species()"
            )

    usageKey = get_usageKey(species_name)

    CABI_tohostPlants = pd.read_csv(
        r"CABI data\CABI_tables\tohostPlants.csv", dtype={"usageKey": "str"}
    )
    CABI_topathwayVectors = pd.read_csv(
        r"CABI data\CABI_tables\topathwayVectors.csv", dtype={"usageKey": "str"}
    )
    CABI_topathwayCauses = pd.read_csv(
        r"CABI data\CABI_tables\topathwayCauses.csv", dtype={"usageKey": "str"}
    )
    CABI_tovectorsAndIntermediateHosts = pd.read_csv(
        r"CABI data\CABI_tables\tovectorsAndIntermediateHosts.csv",
        dtype={"usageKey": "str"},
    )
    EPPO_hosts = pd.read_csv(r"EPPO data\EPPO_hosts.csv", dtype={"usageKey": "str"})
    DAISIE_pathways = pd.read_csv(
        r"DAISIE data\DAISIE_pathways.csv", dtype={"usageKey": "str"}
    )
    DAISIE_vectors = pd.read_csv(
        r"DAISIE data\DAISIE_vectors.csv", dtype={"usageKey": "str"}
    )

    ################################################################
    # CABI Queries
    ################################################################
    ################################################################
    results_dict = {}
    # tohostPlants
    # query tohostPlants for all rows where usageKey = usageKey
    # return a list of all rows where usageKey = usageKey
    # place rows into a dataframe and put into results_dict with key 'CABI_tohostPlants'
    results_dict["CABI_tohostPlants"] = CABI_tohostPlants.loc[
        CABI_tohostPlants["usageKey"] == usageKey
    ]
    # topathwayVectors
    # query topathwayVectors for all rows where usageKey = usageKey
    # return a list of all rows where usageKey = usageKey
    # place rows into a dataframe and put into results_dict with key 'CABI_topathwayVectors'
    results_dict["CABI_topathwayVectors"] = CABI_topathwayVectors.loc[
        CABI_topathwayVectors["usageKey"] == usageKey
    ]

    results_dict["CABI_tovectorsAndIntermediateHosts"] = (
        CABI_tovectorsAndIntermediateHosts.loc[
            CABI_tovectorsAndIntermediateHosts["usageKey"] == usageKey
        ]
    )
    results_dict["EPPO_hosts"] = EPPO_hosts.loc[EPPO_hosts["usageKey"] == usageKey]
    results_dict["DAISIE_pathways"] = DAISIE_pathways.loc[
        DAISIE_pathways["usageKey"] == usageKey
    ]
    results_dict["DAISIE_vectors"] = DAISIE_vectors.loc[
        DAISIE_vectors["usageKey"] == usageKey
    ]
    results_dict["CABI_topathwayCauses"] = CABI_topathwayCauses.loc[
        CABI_topathwayVectors["usageKey"] == usageKey
    ]

    # remove blank dataframes from results_dict
    results_dict = {k: v for k, v in results_dict.items() if not v.empty}

    return results_dict


def get_species_list(
    kingdom=None, phylum=None, taxonomic_class=None, order=None, family=None, genus=None
):
    """
    Retrieves a list of species usage keys from the GBIF backbone invasive dataset
    that match the specified taxonomic criteria.
    Parameters:
    kingdom (str, optional): The kingdom to filter by (e.g., 'Animalia').
    phylum (str, optional): The phylum to filter by (e.g., 'Chordata').
    taxonomic_class (str, optional): The class to filter by (e.g., 'Mammalia').
    order (str, optional): The order to filter by (e.g., 'Carnivora').
    family (str, optional): The family to filter by (e.g., 'Felidae').
    genus (str, optional): The genus to filter by (e.g., 'Panthera').
    Returns:
    list: A list of unique usage keys that match the specified taxonomic criteria.
    """
    GBIF_backbone_invasive = pd.read_csv(r"GBIF data\GBIF_backbone_invasive.csv")

    # CREATE LIST OF USAGE KEYS MATCHING taxonomic CRITERIA
    # returns list of usageKeys matching taxonomic criteria

    if kingdom != None:
        GBIF_backbone_invasive = GBIF_backbone_invasive.loc[
            GBIF_backbone_invasive["kingdom"] == kingdom
        ]
    if phylum != None:
        GBIF_backbone_invasive = GBIF_backbone_invasive.loc[
            GBIF_backbone_invasive["phylum"] == phylum
        ]
    if taxonomic_class != None:
        GBIF_backbone_invasive = GBIF_backbone_invasive.loc[
            GBIF_backbone_invasive["class"] == taxonomic_class
        ]

    if order != None:
        GBIF_backbone_invasive = GBIF_backbone_invasive.loc[
            GBIF_backbone_invasive["order"] == order
        ]
    if family != None:
        GBIF_backbone_invasive = GBIF_backbone_invasive.loc[
            GBIF_backbone_invasive["family"] == family
        ]
    if genus != None:
        GBIF_backbone_invasive = GBIF_backbone_invasive.loc[
            GBIF_backbone_invasive["genus"] == genus
        ]

    # list of unique usageKey from GBIF_backbone_invasive
    usageKey_list = GBIF_backbone_invasive["usageKey"].unique().tolist()

    return usageKey_list


def get_native_ranges(species_name, ISO3=None, check_exists=False):
    """
    Retrieves the native ranges of a given species.
    Parameters:
    species_name (str): The name of the species to query.
    ISO3 (list, optional): A list of ISO3 country codes to check if the species is native. Defaults to None.
    check_exists (bool, optional): If True, checks if the species exists in the database before querying. Defaults to False.
    Returns:
    pandas.DataFrame: If ISO3 is None, returns a DataFrame with columns ['ISO3', 'Source', 'Native', 'Reference', 'bioregion', 'DAISIE_region'] containing native range information.
    pandas.DataFrame: If ISO3 is provided, returns a DataFrame with columns ['ISO3', 'Native', 'src'] indicating whether the species is native to each ISO3 code.
    Raises:
    KeyError: If check_exists is True and the species does not exist in the database.
    TypeError: If ISO3 is not a list of 3 character strings.
    UnboundLocalError: If ISO3 is missing from the bioregion crosswalk.
    Notes:
    - The function reads data from several CSV files: 'all_sources_native_ranges.csv', 'native_range_crosswalk.csv', and 'all_records.csv'.
    - The function uses global variables to store the data read from these CSV files.
    """
    # as default, takes usageKey or species name as string and returns as list of native ISO3 codes
    # if ISO3 is not None, returns True or False if species is native to ISO3 - takes a list of ISO3 as input
    if check_exists == True:
        if not check_species_exists(species_name):
            raise KeyError(
                "Species not in Database. Try checking master list with get_all_species()"
            )
    if "native_ranges" not in globals():
        global native_ranges
        native_ranges = pd.read_csv(
            r"native ranges\all_sources_native_ranges.csv",
            dtype={"usageKey": str},
            low_memory=False,
        )
        # native_ranges = native_ranges.loc[native_ranges["Native"] == True]

    if "native_range_crosswalk" not in globals():
        global native_range_crosswalk
        native_range_crosswalk = pd.read_csv(
            r"native ranges\native_range_crosswalk.csv", low_memory=False
        )
    if "all_records" not in globals():
        global all_records
        all_records = pd.read_csv(
            r"occurrences\all_records.csv", dtype={"usageKey": str}, low_memory=False
        )
    usageKey = get_usageKey(species_name)

    if ISO3 == None:
        records = all_records.loc[all_records["usageKey"] == usageKey]
        # filter records to non-na values of Native
        records = records.loc[records["Native"].notna()]
        # remove records where Source is "Original"
        records = records.loc[records["Source"] != "Original"]

        records = records[["ISO3", "Source", "Native", "Reference"]]
        records["bioregion"] = None
        records["DAISIE_region"] = None

        native_ranges_temp = native_ranges.loc[native_ranges["usageKey"] == usageKey]
        # filter to usageKey, source, bioregion

        native_ranges_temp = native_ranges_temp[
            ["Source", "bioregion", "DAISIE_region", "Reference"]
        ]
        # add column Native with value True
        native_ranges_temp["Native"] = True
        # add column ISO3 with value nan
        native_ranges_temp["ISO3"] = np.nan

        # rename source to Source
        # native_ranges_temp = native_ranges_temp.rename(columns={"Source": "Reference"})
        # add column Source with value 'GITAR native ranges'
        # native_ranges_temp["Source"] = "GITAR native ranges"

        # combine records and native_ranges_temp
        # set reecords bioregion and DAISIE_region to nan

        records = pd.concat([records, native_ranges_temp], ignore_index=True)
        return records

    elif ISO3 != None:
        try:
            # if iso3 is not a list or a three character string, raise error
            if not isinstance(ISO3, list):
                raise TypeError("ISO3 must be a list of 3 character strings")
            native_ranges["usageKey"] = native_ranges["usageKey"].astype(str)
            records = all_records.loc[all_records["usageKey"] == usageKey]
            # filter records to non-na values of Native
            records = records.loc[records["Native"].notna()]
            # remove records where Source is "Original"
            records = records.loc[records["Source"] != "Original"]

            records = records[["ISO3", "Source", "Native", "Reference"]]
            records["bioregion"] = np.nan
            records["DAISIE_region"] = np.nan
            t_f_df = pd.DataFrame(columns=["ISO3", "Native", "src"])
            # create list of all values of bioregion in native ranges matching usageKey

            bioregions = (
                native_ranges.loc[native_ranges["usageKey"] == usageKey]["bioregion"]
                .unique()
                .tolist()
            )
            iso_list = []
            native_list = []
            src_list = []
            for iso3 in ISO3:
                # set native to nan
                native = np.nan

                # else if bioregions list is length 1 or more

                if len(bioregions) > 0:
                    # for each bioregion in bioregions
                    # get bioregions matching iso3 from native_range_crosswalk

                    bioregions_temp = (
                        native_range_crosswalk.loc[
                            native_range_crosswalk["ISO3"] == iso3
                        ]["modified_Bioregion"]
                        .unique()
                        .tolist()
                    )
                    for br in bioregions_temp:
                        # if bioregion is in bioregions

                        if br in bioregions:
                            native = True
                            src = "br"
                            break
                        else:
                            native = False
                            src = "br"
                # else if bioregions list is length 0
                if iso3 in records["ISO3"].values:
                    native = records.loc[records["ISO3"] == iso3]["Native"].values[0]
                    src = "records"
                if len(bioregions) == 0 and iso3 not in records["ISO3"].values:
                    native = np.nan
                    src = "records only - no bioregion found"
                # add row to t_f_df

                iso_list.append(iso3)
                native_list.append(native)
                src_list.append(src)
            new_df = pd.DataFrame(
                {"ISO3": iso_list, "Native": native_list, "src": src_list}
            )
            t_f_df = pd.concat([t_f_df, new_df], ignore_index=True)
            return t_f_df
        except UnboundLocalError:
            print("ISO3 missing from bioregion crosswalk")
            print("please add" + iso3 + "to bioregion crosswalk")
            return None


def get_common_names(species_name, check_exists=False):
    """
    Retrieve common names for a given species from DAISIE and EPPO databases.
    Args:
        species_name (str): The scientific name of the species.
        check_exists (bool, optional): If True, checks if the species exists in the database before proceeding. Defaults to False.
    Raises:
        KeyError: If check_exists is True and the species is not found in the database.
    Returns:
        dict: A dictionary containing DataFrames of common names from DAISIE and EPPO databases, keyed by their source names.
    """
    if check_exists == True:
        if not check_species_exists(species_name):
            raise KeyError(
                "Species not in Database. Try checking master list with get_all_species()"
            )

    usageKey = get_usageKey(species_name)

    DAISIE_vernacular = pd.read_csv(
        r"DAISIE data\DAISIE_vernacular_names.csv",
        dtype={"usageKey": str},
        low_memory=False,
    )
    EPPO_names = pd.read_csv(
        r"EPPO data\EPPO_names.csv", dtype={"usageKey": str}, low_memory=False
    )
    results_dict = {}

    # if usagekey in daisie, print "in daisie"
    print(DAISIE_vernacular.head())
    if usageKey in DAISIE_vernacular["usageKey"].values:
        print("in daisie")

    results_dict["DAISIE_vernacular"] = DAISIE_vernacular.loc[
        DAISIE_vernacular["usageKey"] == usageKey
    ]
    results_dict["EPPO_names"] = EPPO_names.loc[EPPO_names["usageKey"] == usageKey]
    results_dict = {k: v for k, v in results_dict.items() if not v.empty}
    return results_dict


def get_trait_table_list():
    """
    Returns a list of trait table names.

    The trait tables include various datasets related to climate, environments,
    host plants, pathways, vectors, habitats, and other relevant information.

    Returns:
        list: A list of strings representing the names of trait tables.
    """
    return [
        "CABI_rainfall",
        "CABI_airtemp",
        "CABI_climate",
        "CABI_environments",
        "CABI_latitude_altitude",
        "CABI_natural_enemies",
        "CABI_water_tolerances",
        "CABI_wood_packaging",
        "CABI_host_plants",
        "CABI_pathway_vectors",
        "CABI_vectorsAndIntermediateHosts",
        "DAISIE_habitats",
        "CABI_impact_summary",
        "CABI_latitude_altitude_ranges",
        "CABI_symptoms_signs",
        "CABI_threatened_species",
        "EPPO_hosts",
        "EPPO_names",
        "DAISIE_pathways",
        "DAISIE_vectors",
        "DAISIE_vernacular",
    ]


# Function to select a specific trait table
def get_trait_table(table_name, usageKey=None):
    """
    Retrieve a trait table by its name and optionally filter by usageKey.
    This function loads a specified trait table from a CSV file if it has not
    been loaded already. If the table is already loaded, it retrieves it from
    the global namespace. Optionally, it can filter the table rows based on
    the provided usageKey.
    Parameters:
    table_name (str): The name of the trait table to retrieve.
    usageKey (str, optional): The usageKey to filter the table rows. Defaults to None.
    Returns:
    pandas.DataFrame: The requested trait table, optionally filtered by usageKey.
    Raises:
    ValueError: If the table name is not found in the list of available tables.
    ValueError: If the file path for the specified table name is not specified.
    """
    if table_name not in get_trait_table_list():
        raise ValueError(f"Table name '{table_name}' not found.")

    # Check if the table has already been loaded
    if table_name in globals():
        table = globals()[table_name]
    else:
        file_path = None
        if table_name == "CABI_rainfall":
            file_path = "CABI data/CABI_tables/torainfall.csv"
        elif table_name == "CABI_airtemp":
            file_path = "CABI data/CABI_tables/toairTemperature.csv"
        elif table_name == "CABI_latitude_altitude":
            file_path = "CABI data/CABI_tables/tolatitudeAndAltitudeRanges.csv"
        # Add similar conditions for other tables

        if file_path is not None:
            table = pd.read_csv(file_path, dtype={"usageKey": str})
            globals()[table_name] = table
        else:
            raise ValueError(f"File path for table '{table_name}' is not specified.")

    # Filter rows based on usageKey if provided
    if usageKey is not None:
        table = table[table["usageKey"] == usageKey]

    return table


def get_GIATAR_current(data_dir=os.getcwd()):
    """
    Download the latest version of the GIATAR dataset from Zenodo and extract it to the given directory.
    If the files already exist, they will be overwritten.

    Args:
        data_dir (str): The directory where the dataset will be extracted. The default is the current working directory.

    Returns:
        None
    """
    url = "https://zenodo.org/api/records/13138446"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        zip_url = data["files"][0]["links"]["self"]
        print(f"Found dataset at {zip_url}. Downloading GIATAR dataset...")
        zip_response = requests.get(zip_url)
        if zip_response.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(zip_response.content)) as z:
                z.extractall(data_dir)
            print("GIATAR dataset downloaded and extracted successfully.")
        else:
            print(
                f"Failed to download GIATAR zip file. Status code: {zip_response.status_code}"
            )
    else:
        print(
            f"Failed to retrieve GIATAR dataset information. Status code: {response.status_code}"
        )


def get_taxa_by_host(host_name):
    """
    Retrieve a list of taxa that are associated with a given host.

    Args:
        host_name (str): The name of the host to query. This should be the host taxa's partial or full scientific name.

    Returns:
        list: A list of taxa names (canonical name) for taxa associated with matches for the specified host name.
    """
    CABI_hosts = pd.read_csv(
        r"CABI data\CABI_tables\tohostPlants.csv", dtype={"usageKey": "str"}
    )
    EPPO_hosts = pd.read_csv(r"EPPO data\EPPO_hosts.csv", dtype={"usageKey": "str"})

    # Filter the dataframes to get rows where the host name matches
    cabi_hosts = CABI_hosts[
        CABI_hosts["Plant name"].str.contains(host_name, case=False, na=False)
    ]
    eppo_hosts = EPPO_hosts[
        EPPO_hosts["full_name"].str.contains(host_name, case=False, na=False)
    ]
    # Print all matched host names if either cabi_hosts or eppo_hosts has more than one match
    if len(cabi_hosts.index) > 1 or len(eppo_hosts.index) > 1:
        print(f"Host name '{host_name}' matched the following host species:")
        if len(cabi_hosts) > 1:
            print("CABI:")
            print(", ".join(cabi_hosts["Plant name"].unique()))
        if len(eppo_hosts) > 1:
            print("EPPO:")
            print(", ".join(eppo_hosts["full_name"].unique()))

        # Combine the results and get unique taxa
        combined_hosts = pd.concat([cabi_hosts, eppo_hosts])
        taxa_keys = combined_hosts["usageKey"].unique().tolist()
        # Get the canonicalNames associated with these usageKeys
        taxa_list = (
            invasive_all_source.loc[invasive_all_source["usageKey"].isin(taxa_keys)][
                "canonicalName"
            ]
            .unique()
            .tolist()
        )
        # Print the length of the combined list
        print(
            f"Total number of invasive taxa associated with '{host_name}': {len(taxa_list)}"
        )
    else:
        print(f"No host species found for '{host_name}'")
        taxa_list = []
    return taxa_list


def get_taxa_by_pathway(pathway_name):
    """
    Retrieve a list of taxa that are associated with a given pathway.

    Args:
        pathway_name (str): The name of the pathway to query. Pathways are typically common-language key terms or phrases like "ornamental", "wind", or "plant parts".

    Returns:
        list: A list of taxa names (canonical name) for taxa associated with matches for the specified pathway name.
    """
    CABI_pathways = pd.read_csv(
        r"CABI data\CABI_tables\topathwayVectors.csv", dtype={"usageKey": "str"}
    )
    DAISIE_pathways = pd.read_csv(
        r"DAISIE data\DAISIE_pathways.csv", dtype={"usageKey": "str"}
    )
    CABI_pathway_causes = pd.read_csv(
        r"CABI data\CABI_tables\topathwayCauses.csv", dtype={"usageKey": "str"}
    )

    # Filter the dataframes to get rows where the pathway name matches
    # Combine the columns "Vector" and "Notes" to create "pathway"
    CABI_pathways["pathway"] = CABI_pathways["Vector"] + ": " + CABI_pathways["Notes"]
    cabi_pathways = CABI_pathways[
        CABI_pathways["pathway"].str.contains(pathway_name, case=False, na=False)
    ]

    daisie_pathways = DAISIE_pathways[
        DAISIE_pathways["pathway"].str.contains(pathway_name, case=False, na=False)
    ]
    # Combine the columns "Cause" and "Notes" to create "pathway"
    CABI_pathway_causes["pathway"] = (
        CABI_pathway_causes["Cause"] + ": " + CABI_pathway_causes["Notes"]
    )
    cabi_pathway_causes = CABI_pathway_causes[
        CABI_pathway_causes["pathway"].str.contains(pathway_name, case=False, na=False)
    ]
    #

    # Print all matched pathway names if any dataframe has more than one match
    if (
        len(cabi_pathways.index) > 1
        or len(daisie_pathways.index) > 1
        or len(cabi_pathway_causes.index) > 1
    ):
        print(f"Pathway name '{pathway_name}' matched the following pathways:")
        if len(cabi_pathways) > 1:
            print("CABI Pathways:")
            print(", ".join(cabi_pathways["pathway"].unique()))
        if len(daisie_pathways) > 1:
            print("DAISIE Pathways:")
            print(", ".join(daisie_pathways["pathway"].unique()))
        if len(cabi_pathway_causes) > 1:
            print("CABI Pathway Causes:")
            print(", ".join(cabi_pathway_causes["pathway"].unique()))

        # Combine the results and get unique taxa
        combined_pathways = pd.concat(
            [cabi_pathways, daisie_pathways, cabi_pathway_causes]
        )
        taxa_keys = combined_pathways["usageKey"].unique().tolist()
        # Get the canonicalNames associated with these usageKeys
        taxa_list = (
            invasive_all_source.loc[invasive_all_source["usageKey"].isin(taxa_keys)][
                "canonicalName"
            ]
            .unique()
            .tolist()
        )

        # Print the length of the combined list
        print(
            f"Total number of invasive taxa associated with '{pathway_name}': {len(taxa_list)}"
        )
    else:
        print(f"No pathways found matching '{pathway_name}'")
        taxa_list = []
    return taxa_list


def get_taxa_by_vector(vector_name):
    """
    Retrieve a list of taxa that are associated with a given vector.

    Args:
        vector_name (str): The name of the vector to query. This should be the vector species' partial or full scientific name.

    Returns:
        list: A list of taxa names (canonical name) for taxa associated with matches for the specified vector name.
    """
    CABI_vectors = pd.read_csv(
        r"CABI data\CABI_tables\tovectorsAndIntermediateHosts.csv",
        dtype={"usageKey": "str"},
    )

    # Filter the dataframe to get rows where the vector name matches
    cabi_vectors = CABI_vectors[
        CABI_vectors["Vector"].str.contains(vector_name, case=False, na=False)
    ]

    # Print all matched vector names if the dataframe has more than one match
    if len(cabi_vectors.index) > 1:
        print(f"Vector name '{vector_name}' matched the following vectors:")
        print(", ".join(cabi_vectors["Vector"].unique()))

        # Get unique taxa
        taxa_keys = cabi_vectors["usageKey"].unique().tolist()

        # Get the canonicalNames associated with these usageKeys
        taxa_list = (
            invasive_all_source.loc[invasive_all_source["usageKey"].isin(taxa_keys)][
                "canonicalName"
            ]
            .unique()
            .tolist()
        )

        # Print the length of the list
        print(
            f"Total number of invasive taxa associated with '{vector_name}': {len(taxa_list)}"
        )
    else:
        print(f"No vectors found for '{vector_name}'")
        taxa_list = []
    return taxa_list
