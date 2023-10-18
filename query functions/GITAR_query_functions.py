import pandas as pd
import numpy as np
import math
import os
import warnings
import pygbif
import dotenv


# get_species_name(usageKey) - returns species name as string - takes usageKey as string or int

# get_usageKey(species_name) - returns usageKey as string - takes species name as string

# get_all_species() - returns list of all species names in database - no inputs

# check_species_exists(species_name) - returns True or False - takes species name as string

# get_first_introductions(usageKey, check_exists=False, ISO3_only=False, import_additional_native_info=True) - returns dataframe of first introductions - takes usageKey as string or int, check_exists=True will raise a KeyError if species is not in database, ISO3_only=True will return only return species location info that are 3 character ISO3 codes. Some other location info includes bioregions or other geonyms, import_additional_native_info=True will import additional native range info, first by seeing if native range info for a particular country is availible from sources that reported later than the first introduction, and second by importing native range info from the native range database

# get_all_introductions(usageKey, check_exists=False, ISO3_only=True) - returns dataframe of all introductions - takes usageKey as string or int, check_exists=True will raise a KeyError if species is not in database, ISO3_only=True will return only return species location info that are 3 character ISO3 codes. Some other location info includes bioregions or other geonyms

# get_ecology(species_name) - returns dictionary of dataframes of ecology info - takes species name as string

# get_hosts_and_vectors(species_name) - returns dictionary of dataframes of host and vector info - takes species name as string

# get_species_list(kingdom=None, phylum=None, taxonomic_class=None, order=None, family=None, genus=None) - returns list of usageKeys matching taxonomic criteria - takes kingdom, phylum, taxonomic_class, order, family, genus as strings

# get_native_ranges(usageKey, ISO3=None) - returns dataframe of native ranges - takes usageKey as string or int, ISO3=None returns dataframe of native ranges, ISO3=list returns dataframe of native ranges and True or False if species is native to ISO3 - takes a list of ISO3 as input


#### DATA PATH ####
data_path = r"H:\Shared drives\Pandemic Data\Invasive database\Database"

google_root = "H:"
# if .env file exists, get data_path from .env file
if os.path.exists(".env"):
    data_path = dotenv.get_key(".env", "data_path")

os.chdir(data_path)


def create_dotenv(dp):
    # create .env file in current directory
    # write data_path to .env file
    with open(".env", "w") as f:
        f.write(f"data_path={dp}")


# os.chdir("..")
if "invasive_all_source" not in globals():
    global invasive_all_source
    invasive_all_source = pd.read_csv(r"species lists\invasive_all_source.csv")


if "first_records" not in globals():
    global first_records
    first_records = pd.read_csv(r"occurrences\first_records.csv", low_memory=False)


def get_species_name(usageKey):
    # function takes a usageKey as a
    if usageKey in invasive_all_source["usageKey"].values:
        return invasive_all_source.loc[
            invasive_all_source["usageKey"] == usageKey, "canonicalName"
        ].values[0]


def get_usageKey(species_name):
    if "invasive_all_source" not in globals():
        global invasive_all_source
        invasive_all_source = pd.read_csv(f"species lists\invasive_all_source.csv")

    if species_name in invasive_all_source["canonicalName"].values:
        return invasive_all_source.loc[
            invasive_all_source["canonicalName"] == species_name, "usageKey"
        ].values[0]
    elif species_name in invasive_all_source["speciesASFR"].values:
        return invasive_all_source.loc[
            invasive_all_source["speciesASFR"] == species_name, "usageKey"
        ].values[0]
    elif species_name in invasive_all_source["speciesEPPO"].values:
        return invasive_all_source.loc[
            invasive_all_source["speciesEPPO"] == species_name, "usageKey"
        ].values[0]
    elif species_name in invasive_all_source["speciesCABI"].values:
        return invasive_all_source.loc[
            invasive_all_source["speciesCABI"] == species_name, "usageKey"
        ].values[0]
    elif species_name in invasive_all_source["usageKey"].values:
        return species_name

    else:
        try:
            gbif = pygbif.species.name_backbone(name=species_name, rank="species")
            return str(gbif["usageKey"])
        except KeyError:
            print("species not found in Database or GBIF")
            return None


def get_all_species():
    # iterate through all rows in invasive_all_source and return a list of all species names
    # if rank is 'SPECIES', 'FORM', 'SUBSPECIES', 'VARIETY' return canonicalName
    # otherwise if speciesASFR or speciesCABI is not null return that
    # collect the result of all rows into a list and return that
    species_list = []
    for index, row in invasive_all_source.iterrows():
        if row["rank"] in ["SPECIES", "FORM", "SUBSPECIES", "VARIETY"]:
            species_list.append(row["canonicalName"])
        elif pd.notnull(row["speciesEPPO"]):
            species_list.append(row["speciesEPPO"])
        elif pd.notnull(row["speciesASFR"]):
            # if row['speciesASFR'] in species_list, append row['speciesCABI'] instead
            if row["speciesASFR"] in species_list:
                species_list.append(row["speciesCABI"])
            else:
                species_list.append(row["speciesASFR"])
        elif pd.notnull(row["speciesCABI"]):
            species_list.append(row["speciesCABI"])
    return species_list


def check_species_exists(species_name):
    # function takes a species name or usageKey and checks if it exists in the database
    if get_usageKey(species_name) in invasive_all_source["usageKey"].values:
        return True
    else:
        return False


def get_first_introductions(
    usageKey, check_exists=False, ISO3_only=False, import_additional_native_info=True
):
    # check_exists = True will raise a KeyError if species is not in database
    # ISO3_only = True will return only return species location info that are 3 character ISO3 codes. Some other location info includes bioregions or other geonyms
    # import_additional_native_info = True will import additional native range info, first by seeing if native range info for a particular country is availible from sources that reported later than the first introduction, and second by importing native range info from the native range database

    if check_exists == True:
        if not check_species_exists(usageKey):
            raise KeyError(
                "Species not in Database. Try checking master list with get_all_species()"
            )

    if isinstance(usageKey, int):
        usageKey = str(usageKey)

    if not usageKey.isnumeric():
        "getting usageKey from species name"
        usageKey = get_usageKey(usageKey)

    # create df of all first introductinos where usageKey = usageKey

    df = first_records.loc[first_records["usageKey"] == usageKey].copy()
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


def get_all_introductions(
    usageKey, check_exists=False, ISO3_only=True, import_additional_native_info=True
):
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
    all_records = pd.read_csv(r"occurrences\all_records.csv", low_memory=False)

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


def get_ecology(species_name):
    # os.chdir(f"{google_root}\Shared drives\Pandemic Data\Invasive database\Database")
    if not check_species_exists(species_name):
        raise KeyError(
            "Species not in Database. Try checking master list with get_all_species()"
        )
    result_dict = {}

    usageKey = get_usageKey(species_name)

    # load csv for each of these files'CABI_rainfall', 'CABI_airtemp', 'CABI_climate', 'CABI_environments', 'CABI_lat_alt', 'CABI_water_tolerances'

    CABI_rainfall = pd.read_csv(r"CABI data\CABI_tables\torainfall.csv")
    CABI_airtemp = pd.read_csv(r"CABI data\CABI_tables\toairTemperature.csv")
    CABI_climate = pd.read_csv(r"CABI data\CABI_tables\toclimate.csv")
    CABI_environments = pd.read_csv(r"CABI data\CABI_tables\toenvironments.csv")
    CABI_lat_alt = pd.read_csv(r"CABI data\CABI_tables\tolatitudeAndAltitudeRanges.csv")
    CABI_natural_enemies = pd.read_csv(r"CABI data\CABI_tables\tonaturalEnemies.csv")
    CABI_water_tolerances = pd.read_csv(r"CABI data\CABI_tables\towaterTolerances.csv")
    CABI_wood_packaging = pd.read_csv(r"CABI data\CABI_tables\towoodPackaging.csv")

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
    # remove empty keys in result_dict
    result_dict = {k: v for k, v in result_dict.items() if not v.empty}

    return result_dict


def get_hosts_and_vectors(species_name):
    # os.chdir(f"{google_root}\Shared drives\Pandemic Data\Invasive database\Database")

    if not check_species_exists(species_name):
        raise KeyError(
            "Species not in Database. Try checking master list with get_all_species()"
        )

    usageKey = get_usageKey(species_name)

    CABI_tohostPlants = pd.read_csv(r"CABI data\CABI_tables\tohostPlants.csv")
    CABI_topathwayVectors = pd.read_csv(r"CABI data\CABI_tables\topathwayVectors.csv")
    CABI_tovectorsAndIntermediateHosts = pd.read_csv(
        r"CABI data\CABI_tables\tovectorsAndIntermediateHosts.csv"
    )
    EPPO_hosts = pd.read_csv(r"EPPO data\EPPO_hosts.csv")

    # convert usageKey to string for every dataframe
    CABI_tohostPlants["usageKey"] = CABI_tohostPlants["usageKey"].astype(str)
    CABI_topathwayVectors["usageKey"] = CABI_topathwayVectors["usageKey"].astype(str)
    CABI_tovectorsAndIntermediateHosts["usageKey"] = CABI_tovectorsAndIntermediateHosts[
        "usageKey"
    ].astype(str)
    # truncate decimals from string column EPPO_hosts usageKey
    EPPO_hosts["usageKey"] = EPPO_hosts["usageKey"].str.split(".").str[0]

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
    results_dict[
        "CABI_tovectorsAndIntermediateHosts"
    ] = CABI_tovectorsAndIntermediateHosts.loc[
        CABI_tovectorsAndIntermediateHosts["usageKey"] == usageKey
    ]
    results_dict["EPPO_hosts"] = EPPO_hosts.loc[EPPO_hosts["usageKey"] == usageKey]

    # remove blank dataframes from results_dict
    results_dict = {k: v for k, v in results_dict.items() if not v.empty}

    return results_dict


def get_species_list(
    kingdom=None, phylum=None, taxonomic_class=None, order=None, family=None, genus=None
):
    os.chdir(f"{google_root}\Shared drives\Pandemic Data\Invasive database\Database")
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


def get_native_ranges(usageKey, ISO3=None):
    # as default, takes usageKey or species name as string and returns as list of native ISO3 codes
    # if ISO3 is not None, returns True or False if species is native to ISO3 - takes a list of ISO3 as input

    # os.chdir(f"{google_root}\Shared drives\Pandemic Data\Invasive database\Database")

    if "native_ranges" not in globals():
        global native_ranges
        native_ranges = pd.read_csv(
            r"native ranges\all_sources_native_ranges.csv", low_memory=False
        )
    if "native_range_crosswalk" not in globals():
        global native_range_crosswalk
        native_range_crosswalk = pd.read_csv(
            r"native ranges\native_range_crosswalk.csv", low_memory=False
        )
    if "all_records" not in globals():
        global all_records
        all_records = pd.read_csv(r"occurrences\all_records.csv", low_memory=False)

    if isinstance(usageKey, int):
        usageKey = str(usageKey)
    if not usageKey.isnumeric():
        usageKey = str(get_usageKey(usageKey))
    if ISO3 == None:
        records = all_records.loc[all_records["usageKey"] == usageKey]
        # filter records to non-na values of Native
        records = records.loc[records["Native"].notna()]
        # remove records where Source is "Original"
        records = records.loc[records["Source"] != "Original"]

        records = records[["ISO3", "Source", "Native", "Reference"]]
        records["bioregion"] = None
        records["DAISIE_region"] = None

        native_ranges["usageKey"] = native_ranges["usageKey"].astype(str)
        native_ranges_temp = native_ranges.loc[native_ranges["usageKey"] == usageKey]
        # filter to usageKey, source, bioregion

        native_ranges_temp = native_ranges_temp[
            ["Source", "bioregion", "DAISIE_region", "Reference"]
        ]
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
        """
        native_ranges["usageKey"] = native_ranges["usageKey"].astype(str)
        native_ranges_temp = native_ranges.loc[native_ranges["usageKey"] == usageKey]
        # filter to usageKey, source, bioregion

        native_ranges_temp = native_ranges_temp[
            ["Source", "bioregion", "DAISIE_region", "Reference"]
        ]
        # rename source to Source
        # native_ranges_temp = native_ranges_temp.rename(columns={"Source": "Reference"})
        # add column Source with value 'GITAR native ranges'
        # native_ranges_temp["Source"] = "GITAR native ranges"

        # combine records and native_ranges_temp
        # set reecords bioregion and DAISIE_region to na

        records = records.append(native_ranges_temp)
        """


"""
    bioregions_list = native_ranges["bioregion"].unique().tolist()
    for br in bioregions_list:
        # use crosswalk to get all ISO3 matching bioregion
        if br in native_range_crosswalk["bioregion"].values:
            ISO3_df = native_range_crosswalk.loc[
                native_range_crosswalk["bioregion"] == br, "ISO3"
            ]
        # if native_ranges_df does not exist, create it
        if "native_ranges_df" not in locals():
            native_ranges_df = pd.DataFrame()
            # native ranges df is a dataframe of all records in native_ranges_temp where bioregion = br

        # for each unique ISO3 in ISO3_df, add a row to native_ranges_df with source and bioregion from native_ranges_temp where bioregion = br and ISO3 = ISO3 and column "database" = 'GITAR native ranges'
        for iso in ISO3_df:
            # create new row
            new_row = native_ranges_temp.loc[native_ranges_temp["bioregion"] == br]
            # add ISO3 to new row
            new_row["ISO3"] = iso
            # add database to new row
            new_row["database"] = "GITAR native ranges"
            # rename source to Source
            new_row = new_row.rename(columns={"source": "Source"})

            # add new row to native_ranges_df
            native_ranges_df = native_ranges_df.append(new_row)

    # append native_ranges_df to records
    records = records.append(native_ranges_df)

    # return list of bioregion names where usageKey = usageKey
    # select rows of native_ranges where usageKey = usageKey
    # set native_ranges usageKey to string

    return nr_list
"""
