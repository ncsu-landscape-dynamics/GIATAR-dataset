"""
File: data_update/data_functions.py
Author: Ariel Saffer
Date created: 2023-04-14
Description: Helper functions for data_update scripts
"""

import pandas as pd
import numpy as np

import requests
from urllib.request import urlopen
from bs4 import BeautifulSoup
import ssl
import urllib
from time import sleep
from datetime import date

import pycountry

import spacy
import regex as re
import tqdm
import os
import dotenv
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from pygbif import species
from urllib.error import HTTPError, URLError
from urllib3 import Timeout
from tqdm._tqdm_notebook import tqdm_notebook


dotenv.load_dotenv(".env")

data_dir = os.getenv("DATA_PATH")

# Get today's date as date updated
today = date.today()

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

### EPPO functions

# Define all query options

general = ""
names = f"/names"
taxonomy = f"/taxonomy"
categorization = f"/categorization"
hosts = f"/hosts"

# Function to call all queries from API - output as pd dataframe


def eppo_api(code, query, token):
    # API URL base
    root = "https://data.eppo.int/api/rest/1.0/taxon/"
    auth = f"?authtoken={token}"

    call = f"{root}{code}{query}{auth}"

    # Get the API call response
    try:
        response = requests.get(call).json()
    except requests.exceptions.RequestException:
        print("Just a second...")
        sleep(5)
        try:
            response = requests.get(call, verify=False).json()
        except requests.exceptions.RequestException:
            print("Trying a minute...")
            sleep(20)
            response = requests.get(call).json()
    # Process the response

    try:
        response["message"] == "This service does not exists"
        return np.nan
    except:
        if len(response) == 0:
            return None
        if query == hosts:
            list = []
            for section in response:
                list.append(response[section])
            response_table = pd.DataFrame.from_dict(list[0])

            for table in list[1:]:
                response_table = pd.concat(
                    [response_table, pd.DataFrame.from_dict(table)]
                )
        else:
            response_table = pd.DataFrame.from_dict(response)
    return response_table


# Function to check for EPPO categorization


def eppo_cat_api(code, token):
    categorization = f"/categorization"

    root = "https://data.eppo.int/api/rest/1.0/taxon/"
    auth = f"?authtoken={token}"
    try:
        response = requests.get(f"{root}{code}{categorization}{auth}").json()
    except requests.exceptions.RequestException:
        sleep(5)
        try:
            response = requests.get(
                f"{root}{code}{categorization}{auth}", verify=False
            ).json()
        except requests.exceptions.RequestException:
            sleep(20)
            response = requests.get(f"{root}{code}{categorization}{auth}").json()
    return response


# Functions to scrape EPPO reports from an individual species page


def scrape_eppo_reports_species(code):
    # Ignore SSL certificate errors
    url = f"https://gd.eppo.int/taxon/{code}/reporting"
    try:
        html = urlopen(url, context=ctx).read()
    except urllib.error.HTTPError as err:
        if err.code == 404:
            return np.nan
        else:
            print("Waiting a moment...")
            sleep(25)
            html = urlopen(url, context=ctx).read()

    soup = BeautifulSoup(html, "html.parser")

    reporting = soup.find("table")
    report_table = pd.read_html(reporting.prettify())[0]
    report_table["codeEPPO"] = code
    links = reporting.find_all("a")

    report_links = []

    for link in links:
        report_links.append("https://gd.eppo.int" + link.get("href"))

    report_table["links"] = report_links

    return report_table


def eppo_query_wrapper(eppo_species, query, token, append=False):
    codes = eppo_species["codeEPPO"].unique()

    print(f"Querying EPPO for {query} data for {len(codes)} species...")

    read_tables = []

    for i, code in enumerate(codes):
        table = eppo_api(code, query, token)
        i += 1
        if table is None:
            continue
        if table is np.nan:
            continue
        table["codeEPPO"] = code
        table["usageKey"] = eppo_species.loc[
            eppo_species["codeEPPO"] == code
        ].usageKey.values[0]

        read_tables.append(table)

        if i % 100 == 0:
            print(f"{i} out of {len(codes)} done!")

    if len(read_tables) < 1:
        print(f"No data for {query} found!")

    else:
        section_table = read_tables[0]

        for table in range(1, len(read_tables)):
            section_table = pd.concat([section_table, read_tables[table]])

        section_table["Date"] = f"{today.year}-{today.month:02d}-{today.day:02d}"

        if append == True:
            try:
                prev_table = pd.read_csv(f"{data_dir}/EPPO data/EPPO_{query[1:]}.csv")
                section_table = pd.concat(
                    [prev_table, section_table], ignore_index=True
                ).drop_duplicates(subset=section_table.columns.difference(["Date"]))
            except FileNotFoundError:
                print(f"No previous {query} data found.")

        section_table.to_csv(f"{data_dir}/EPPO data/EPPO_{query[1:]}.csv", index=False)

        # Instead of re-writing, do we want to consolidate? I'm not sure...

        print(
            f'File for "{query}" complete! Species: {len(section_table.codeEPPO.unique())}, Rows: {len(section_table.index)}'
        )

    return None


# Using Geopolitical entities from Spacy Named Entity Recognition from EPPO titles


def spacy_place(text):
    nlp = spacy.load("en_core_web_sm")
    doc = nlp(text)
    places = []
    for ent in doc.ents:
        if ent.label_ == "GPE":
            places.append(ent.text)
    if len(places) > 0:
        return places
    else:
        return None


# Since reports include negative records (e.g. Incursion and eradication of Fusarium oxysporu....)
# limit to explicit new records until we incorporate semantic NLP


def get_record(title):
    if "First report of" in title:
        return True
    elif "New finding of" in title:
        return True
    else:
        return False


# Wrapper function to get a country column from a table of EPPO reports with titles


def country_from_eppo_reports(section_table):
    # Apply Geopolitical entities from Spacy Named Entity Recognition to all titles
    section_table["place_list"] = section_table.apply(
        lambda x: spacy_place(x["Title"]), axis=1
    )
    section_table["year"] = section_table.apply(lambda x: x["year-month"][0:4], axis=1)

    # Expand multiple places from one report into individual rows
    section_table = section_table.explode("place_list")
    section_table = section_table.loc[section_table["place_list"].notna()]

    section_table.reset_index(drop=True, inplace=True)

    # Merge in known country codes - names
    countries = pd.read_csv(data_dir + "country files/country_codes.csv")
    section_table = pd.merge(
        left=section_table,
        right=countries,
        how="left",
        left_on="place_list",
        right_on="NAME",
    )

    section_table.rename(columns={"place_list": "location"}, inplace=True)

    # Use pycountry fuzzy matching for unmatched locations
    dicts = {}
    ISO3_codes = []
    unique_countries = section_table.loc[section_table["ISO3"].isna()].location.unique()
    for country in unique_countries:
        ISO3_codes.append(get_ISO3(country))
    for i in range(len(unique_countries)):
        dicts[unique_countries[i]] = ISO3_codes[i]
    section_table.loc[section_table["ISO3"].isna(), "ISO3"] = section_table.loc[
        section_table["ISO3"].isna()
    ].apply(lambda x: dicts.get(x.location), axis=1)

    # Clean up un-needed columns and empty records (non-countries)
    section_table.drop(columns=["NAME", "is_record", "ISO2"], inplace=True)
    section_table = section_table.loc[section_table["ISO3"] != "Not found"]

    return section_table


# Function to scrape new sequential EPPO reports from the EPPO reporting service page


def scrape_monthly_eppo_report(year, month):
    # Ignore SSL certificate errors
    url = f"https://gd.eppo.int/reporting/Rse-{year}-{month}"
    try:
        html = urlopen(url, context=ctx).read()
    except urllib.error.HTTPError as err:
        if err.code == 404:
            return np.nan
        else:
            print("Waiting a moment...")
            sleep(25)
            html = urlopen(url, context=ctx).read()

    soup = BeautifulSoup(html, "html.parser")

    reporting = soup.find("table")
    report_table = pd.read_html(reporting.prettify())[0]
    report_table["year-month"] = f"{year}-{month}"
    report_table["year"] = year
    links = reporting.find_all("a")

    report_links = []

    for link in links:
        report_links.append("https://gd.eppo.int" + link.get("href"))

    report_table["links"] = report_links

    return report_table


# Extract species from EPPO reporting service page reports


def get_species(title):
    if "First report of" in title:
        return (
            re.search(r"First report of (.*?) (in|from)", title)
            .group(1)
            .replace("’", "'")
            .replace("‘", "'")
            .lower()
        )
    elif "New finding of" in title:
        return (
            re.search(r"New finding of (.*?) (in|from)", title)
            .group(1)
            .replace("’", "'")
            .replace("‘", "'")
            .lower()
        )
    else:
        return None


# Web-scrape first record data from EPPO species distribution pages

# Internal function to get the year and type and references


def get_distribution_data(url):
    # Ignore SSL certificate errors
    try:
        html = urlopen(url, context=ctx).read()
    except urllib.error.HTTPError as err:
        if err.code == 404:
            return np.nan
        else:
            print("Waiting a moment...")
            sleep(25)
            html = urlopen(url, context=ctx).read()

    soup = BeautifulSoup(html, "html.parser")
    soup_text = soup.text
    intro_years = re.findall(r"First recorded in: *([0-9]*)", soup.get_text())
    type = "First report"

    if len(intro_years) == 0:
        intro_years = re.search(r"\b([0-9]{4})\b", soup.get_text())
        type = "First year listed"

    comments = ""
    references = ""

    if re.search("Situation in neighbouring countries", soup_text):
        if re.search("References", soup_text):
            references = soup_text[
                re.search("References", soup_text)
                .span()[1] : re.search("Situation in neighbouring countries", soup_text)
                .span()[0]
            ].strip()
            if re.search("Comments", soup_text):
                comments = soup_text[
                    re.search("Comments", soup_text)
                    .span()[1] : re.search("References", soup_text)
                    .span()[0]
                ].strip()
        elif re.search("Comments", soup_text):
            comments = soup_text[
                re.search("Comments", soup_text)
                .span()[1] : re.search("Situation in neighbouring countries", soup_text)
                .span()[0]
            ].strip()
    elif re.search("References", soup_text):
        references = soup_text[
            re.search("References", soup_text)
            .span()[1] : re.search("Contact EPPO", soup_text)
            .span()[0]
        ].strip()
        if re.search("Comments", soup_text):
            comments = soup_text[
                re.search("Comments", soup_text)
                .span()[1] : re.search("References", soup_text)
                .span()[0]
            ].strip()
    elif re.search("Comments", soup_text):
        comments = soup_text[
            re.search("Comments", soup_text)
            .span()[1] : re.search("Contact EPPO", soup_text)
            .span()[0]
        ].strip()

    # Find references: Pattern (YYYY) and keep just YYYY
    reference_years = re.findall(r"\(([0-9]{4})\)", references) + re.findall(
        r"\(([0-9]{4})/", comments
    )
    reference_years = [int(year) for year in reference_years]
    if len(reference_years) > 0:
        earliest_reference = min(reference_years)
    else:
        earliest_reference = ""

    # Combine references and comments
    if len(references) > 0:
        if len(comments) > 0:
            combined_references = comments + "\n" + references
        else:
            combined_references = references
    else:
        combined_references = comments

    return intro_years[0], type, earliest_reference, combined_references


# Function to get the distribution page, table, and access each location's
# distribution page to extract the first recorded year


def scrape_eppo_distribution_species(code):
    # Ignore SSL certificate errors
    url = f"https://gd.eppo.int/taxon/{code}/distribution"
    try:
        html = urlopen(url, context=ctx).read()
    except urllib.error.HTTPError as err:
        if err.code == 404:
            return np.nan
        else:
            print("Waiting a moment...")
            sleep(25)
            html = urlopen(url, context=ctx).read()

    soup = BeautifulSoup(html, "html.parser")

    reporting = soup.find("table")
    report_table = pd.read_html(reporting.prettify())[0]
    report_table["codeEPPO"] = code
    links = reporting.find_all("a")

    report_links = []

    for link in links:
        report_links.append("https://gd.eppo.int" + link.get("href"))

    if len(report_links) > 0:
        report_table["link"] = report_links
        report_table["ISO2"] = report_table.link.str[-2:]

        report_table["First record data"] = report_table.link.apply(
            get_distribution_data
        )

        report_table[
            ["First date", "First date type", "First reference", "References"]
        ] = pd.DataFrame(
            report_table["First record data"].to_list(), index=report_table.index
        )

        report_table = report_table.drop(columns=["First record data"])

    return report_table


#### GBIF functions

# GBIF Match API call: exact and fuzzy matching of species name to GBIF codes
# Fields of interest "usageKey": ,"scientificName": ,"canonicalName": ,"rank": ,"confidence":97, "matchType"


def write_gbif_match(species):
    call = f"https://api.gbif.org/v1/species/match?verbose=true&name={species}"
    return call


# Upack the response (JSON) into just the country - count values


def call_gbifmatch_api(call):
    # To deal with API response/retry errors - probably a better way to do this
    try:
        response = requests.get(call).json()
    except requests.exceptions.RequestException:
        sleep(5)
        try:
            response = requests.get(call, verify=False).json()
        except requests.exceptions.RequestException:
            sleep(20)
            response = requests.get(call).json()
    # If a match is found, unpack. If not, fill None
    try:
        usageKey = response["usageKey"]
        scientificName = response["scientificName"]
        canonicalName = response["canonicalName"]
        rank = response["rank"]
        confidence = response["confidence"]
        matchType = response["matchType"]
    except:
        usageKey = None
        scientificName = None
        canonicalName = None
        rank = None
        confidence = None
        matchType = None
    return [usageKey, scientificName, canonicalName, rank, confidence, matchType]


# Write, call, and unpack into df columns


def gbif_species_match(df):
    df["api_call"] = df.species.apply(write_gbif_match)

    responses = []
    for call in df.api_call:
        response = call_gbifmatch_api(call)
        responses.append(response)
    df["responses"] = responses
    # Or,
    # df['responses'] = df.api_call.apply(call_gbifmatch_api)
    # Expanding the results into lists of countries and counts
    df[
        [
            "usageKey",
            "scientificName",
            "canonicalName",
            "rank",
            "confidence",
            "matchType",
        ]
    ] = df.responses.apply(pd.Series)
    df.drop(columns=["responses", "api_call"], inplace=True)


# GBIF API call: occurrence status = present, count for each species/year, for all countries


def write_gbif_counts(df):
    call = f"https://api.gbif.org/v1/occurrence/search?year={df['years']}&occurrence_status=present&taxonKey={df['species']}&facet=country&facetlimit=300&limit=0"
    return call


# Unpack the response (JSON) into just the country - count values
def call_gbif_api(call):
    try:
        response = requests.get(call).json()
    except requests.exceptions.RequestException:
        print("Just a second...")
        sleep(5)
        try:
            response = requests.get(call, verify=False).json()
        except requests.exceptions.RequestException:
            print("Trying a minute...")
            sleep(60)
            try:
                response = requests.get(call).json()
            except requests.exceptions.RequestException:
                print(
                    "Something seems to be wrong with the server... let's take a break."
                )
                sleep(3600)
                response = requests.get(call).json()
    response_vals = response["facets"][0]["counts"]
    country = []
    counts = []
    for i in range(0, len(response_vals)):
        country.append(response_vals[i]["name"])
        counts.append(response_vals[i]["count"])
    return [country, counts]


### CABI functions


def CABI_scrape_invasive(CABI_species):
    for i in CABI_species.index:
        code = CABI_species.loc[i, "codeCABI"]
        url = f"https://www.cabi.org/isc/datasheet/{code}"
        # url = input('Enter - ')
        try:
            html = urlopen(url, context=ctx).read()
        except (urllib.error.HTTPError, urllib.error.URLError):
            print("Just a moment...")
            sleep(5)
            try:
                html = urlopen(url, context=ctx).read()
            except (urllib.error.HTTPError, urllib.error.URLError):
                print("May be a minute...")
                sleep(20)
                try:
                    html = urlopen(url, context=ctx).read()
                except (urllib.error.HTTPError, urllib.error.URLError):
                    print("It's a real webpage error!")
                    CABI_species.loc[i, "invasive"] = "Webpage error"
                    continue

        soup = BeautifulSoup(html, "html.parser")

        # <meta name="datasheettype" content="Invasive Species; Pest; Natural Enemy" />

        # Getting the datasheet type

        datasheet_type = soup.find(attrs={"name": "datasheettype"}).get("content")
        CABI_species.loc[i, "datasheet_type"] = datasheet_type

        # Determine if invasive/pest

        if datasheet_type.find("Invasive species") >= 0:
            CABI_species.loc[i, "invasive"] = True
            CABI_species["scrape"][i] = soup
        elif datasheet_type.find("Pest") >= 0:
            CABI_species.loc[i, "invasive"] = True
            CABI_species["scrape"][i] = soup
        else:
            CABI_species.loc[i, "invasive"] = False

        if i % 50 == 0:
            print(f"{i} out of {len(CABI_species['code'])} done!")

    return None


def unpack_CABI_scrape(scrape):
    soup = BeautifulSoup(scrape, "html.parser")

    # Sections of interest in CABI are either Expanded or Collapsed
    sections_content = soup.find_all(
        attrs={"class": "Product_data-item Section_Expanded"}
    ) + soup.find_all(attrs={"class": "Product_data-item Section_Collapsed"})

    sections = []

    for section in sections_content:
        sections.append(section.get("id"))

    content = []
    is_table = []

    for i in range(0, len(sections)):
        section_content = sections_content[i]
        table = section_content.find("table")
        if table is None:
            is_table.append(False)
            content.append(section_content.text)
        else:
            is_table.append(True)
            content.append(str(section_content))

    return [sections, content, is_table]


def CABI_sections_to_tables(CABI_tables, append=False):
    sections = CABI_tables.loc[~CABI_tables["section"].isnull()].section.unique()

    for section in sections:
        sub_section = CABI_tables.loc[CABI_tables["section"] == section].reset_index()
        read_tables = []

        for i in sub_section.index:
            tables = pd.read_html(sub_section.content[i])

            for table in tables:
                table["code"] = sub_section.code[i]
                table["usageKey"] = sub_section.usageKey[i]
                table["section"] = sub_section.section[i]

                read_tables.append(table)

        section_table = read_tables[0]

        for table in range(1, len(read_tables)):
            section_table = pd.concat([section_table, read_tables[table]])

        section_table["Date"] = f"{today.year}-{today.month:02d}-{today.day:02d}"

        if append == True:
            prev_table = pd.read_csv(f"{data_dir}/CABI data/CABI_tables/{section}.csv")
            section_table = pd.concat(
                [prev_table, section_table], ignore_index=True
            ).drop_duplicates(subset=section_table.columns.difference(["Date"]))

        section_table.to_csv(
            f"{data_dir}/CABI data/CABI_tables/{section}.csv", index=False
        )
        print(
            f'File for "{section}" complete! Species: {len(section_table.code.unique())}, Rows: {len(section_table.index)}'
        )

    return None


### DAISIE functions

daisie_year_map = {
    "90`s ": "1990",
    "Unknown": None,
    "unknown": None,
    "since long": "700",
    "Since long": "700",
    "20. century": "1950",
    "19th century": "1850",
}


def clean_DAISIE_year(year):
    # DAISIE years contain a mix of values - some are single years, some are ranges
    # Some include descriptions like "before 2000" or "probabbly around 1960 by symptoms"
    # Some are missing values (?, 0, .)
    # Function to clean each value to represent a single year as a float
    if len(str(year)) > 3:
        try:
            first_year = float(year)
        except:
            # Look for all 4 digit numbers in the string and take the lowest value
            # If a year is a range, take the start year. If a year is a description, take the year from the description.
            years = re.findall(r"[0-9]{4}", year)
            try:
                first_year = min(years)
            except:
                try:
                    # Use the map for the remaining irregular names
                    first_year = daisie_year_map[year]
                except:
                    first_year = None
    else:
        first_year = None
    return first_year


#### Utility functions

# Handle NAs with pycountry fuzzy search


def get_ISO3(loc):
    if loc == loc:
        try:
            return pycountry.countries.search_fuzzy(loc)[0].alpha_3
        except LookupError:
            if "the " in loc:
                try:
                    return pycountry.countries.search_fuzzy(loc.replace("the", ""))[
                        0
                    ].alpha_3
                except LookupError:
                    return "Not found"
            elif " (" in loc:
                try:
                    return pycountry.countries.search_fuzzy(loc.split(" (")[0])[
                        0
                    ].alpha_3
                except LookupError:
                    return "Not found"
            else:
                return "Not found"
    else:
        return np.nan


# Create a function that cleans the ASFR countries

# Countries not mapped correctly to ISO3 due to
# naming conventions, sub-national locations
missed_countries_dict = {
    "Congo, Democratic Republic of the": "COD",
    "Congo, Republic of the": "COG",
    "Congo, Republic of": "COG",
    "Virgin Islands, US": "VIR",
    "Trinidad-Tobago": "TTO",
    "Laos": "LAO",
    "Macau": "MAC",
    "Saint Paul (France)": "FRA",
    "USACanada": ["USA", "CAN"],
    "Czechoslovakia": "CZE",
    "England": "GBR",
    "Scotland": "GBR",
    "Bonaire, Saint Eustatius and Saba": "BES",
    "Bosnia-Herzegovina": "BIH",
    "Gilbraltar": "GIB",
    "Russian Far East": "RUS",
    "European part of Russia": "RUS",
    "Union of Soviet Socialist Republics": "RUS",
    "Northwestern U.S.A.": "USA",
    "Southwestern U.S.A.": "USA",
    "Southeastern U.S.A.": "USA",
    "Northeastern U.S.A.": "USA",
    "South-Central U.S.A.": "USA",
    "North-Central U.S.A.": "USA",
    "Western Canada": "CAN",
    "Eastern Canada": "CAN",
}


def clean_country_name(country):
    # For cleaning unmatched ISO3 codes
    if len(country) > 3:
        # First check if it is in the missed countries dict
        try:
            return missed_countries_dict[country]
        except KeyError:
            # Clean the country name
            # Replace "Uk" as a word with "United Kingdom"
            country = re.sub("\\bUk\\b", " United Kingdom", country)
            # Split on ", " or " and " if "island" is not in country
            if (", " in country) | (" and " in country) & (
                "island" not in country.lower()
            ):
                country_list = re.split(", | and ", country)
                # Search for the ISO code for each country in the list
                ISO3_list = [get_ISO3(country) for country in country_list]
                return ISO3_list
            else:
                return country
    else:
        return country


# Wrapper function to match countries


def match_countries(df):
    dicts = {}
    ISO3_codes = []
    unique_countries = df.loc[
        (df["ISO3"].isna()) & (df["location"].notna())
    ].location.unique()
    for country in unique_countries:
        ISO3_codes.append(get_ISO3(country))
    for i in range(len(unique_countries)):
        dicts[unique_countries[i]] = ISO3_codes[i]
    df.loc[df["ISO3"].isna(), "ISO3"] = df.loc[df["ISO3"].isna()].apply(
        lambda x: dicts.get(x.location), axis=1
    )
    # Apply the clean country name function for common isuses
    df.loc[df["ISO3"] == "Not found", "ISO3"] = df.loc[df["ISO3"] == "Not found"].apply(
        lambda x: clean_country_name(x.location), axis=1
    )
    # Explode any rows that may contain lists
    df = df.explode("ISO3")
    print("The following location names remain unmatched:")
    print(df.loc[df["ISO3"].str.len() > 3].ISO3.unique())
    return df


### Taxonomic matching functions
### Author: Thom Worm


# Retry decorator to handle HTTP and timeout errors
@retry(
    stop=stop_after_attempt(5),
    wait=wait_fixed(5),
    retry=retry_if_exception_type((HTTPError, Timeout)),
)
def get_species_name_backbone(taxon, strict):
    return species.name_backbone(taxon, verbose=True, strict=strict)


def strip_author_name(taxon):
    # Return the first two words of the species name string
    return " ".join(taxon.split()[:2])


def check_gbif_tax_secondary(dat):
    # Initialize new columns
    dat["scientificName"] = None
    dat["Taxon"] = dat["Taxon_orig"]
    dat["GBIFstatus"] = "Missing"
    dat["GBIFmatchtype"] = None
    dat["GBIFnote"] = None
    dat["GBIFstatus_Synonym"] = None
    dat["species"] = None
    dat["genus"] = None
    dat["family"] = None
    dat["class"] = None
    dat["order"] = None
    dat["phylum"] = None
    dat["kingdom"] = None
    dat["GBIFtaxonRank"] = None
    dat["GBIFusageKey"] = None
    dat["note"] = None

    # Determine taxlist based on columns available
    if "kingdom_user" in dat.columns:
        taxlist_lifeform = dat[["Taxon", "kingdom_user"]].drop_duplicates()
        taxlist = taxlist_lifeform["Taxon"].unique()
    elif "Author" in dat.columns:
        taxlist = (
            dat[["Taxon", "Author"]]
            .drop_duplicates()
            .apply(lambda x: " ".join(x), axis=1)
            .unique()
        )
    else:
        taxlist = dat["Taxon"].unique()

    n_taxa = len(taxlist)

    mismatches = pd.DataFrame(columns=["Taxon", "status", "matchType"])

    # Initialize progress bar
    # tqdm_notebook.pandas()

    for j in range(n_taxa):
        taxon = taxlist[j]
        ind_tax = dat.index[dat["Taxon_orig"] == taxon]
        taxon = (
            taxon.replace(" sp.", " ")
            .replace(" spp.", " ")
            .replace(" .f ", " ")
            .replace(" .var", "")
        )
        try:
            db_all = get_species_name_backbone(taxon, strict=True)
        except (HTTPError, Timeout):
            print(f"Failed to retrieve data for {taxon} after 5 attempts. Skipping.")
            continue
        db = {k: v for k, v in db_all.items() if k != "alternatives"}
        alternatives = db_all.get("alternatives", [])
        print(alternatives)
        if (
            db.get("status") == "ACCEPTED" and db.get("matchType") == "EXACT"
        ):  # exact match
            dat.loc[ind_tax, "Taxon"] = db.get("canonicalName")
            dat.loc[ind_tax, "scientificName"] = db.get("scientificName")
            dat.loc[ind_tax, "GBIFstatus"] = db.get("status")
            dat.loc[ind_tax, "GBIFmatchtype"] = db.get("matchType")
            dat.loc[ind_tax, "GBIFtaxonRank"] = db.get("rank")
            dat.loc[ind_tax, "GBIFusageKey"] = db.get("usageKey")

            dat.loc[ind_tax, "species"] = db.get("species")
            dat.loc[ind_tax, "genus"] = db.get("genus")
            dat.loc[ind_tax, "family"] = db.get("family")
            dat.loc[ind_tax, "class"] = db.get("class")
            dat.loc[ind_tax, "order"] = db.get("order")
            dat.loc[ind_tax, "phylum"] = db.get("phylum")
            dat.loc[ind_tax, "kingdom"] = db.get("kingdom")
            dat.loc[ind_tax, "note"] = "Exact match"
            continue

        elif (
            db.get("status") == "SYNONYM"
            and db.get("matchType") == "EXACT"
            and ("species" in db or "genus" in db)
        ):
            dat.loc[ind_tax, "GBIFstatus"] = db.get("status")
            dat.loc[ind_tax, "GBIFmatchtype"] = db.get("matchType")
            dat.loc[ind_tax, "GBIFtaxonRank"] = db.get("rank")
            dat.loc[ind_tax, "GBIFusageKey"] = db.get("usageKey")

            if any(
                alt.get("status") == "ACCEPTED" and alt.get("matchType") == "EXACT"
                for alt in alternatives
            ):
                accepted_alt = next(
                    alt
                    for alt in alternatives
                    if alt.get("status") == "ACCEPTED"
                    and alt.get("matchType") == "EXACT"
                )
                dat.loc[ind_tax, "scientificName"] = accepted_alt.get("scientificName")
                dat.loc[ind_tax, "Taxon"] = accepted_alt.get("canonicalName")

                dat.loc[ind_tax, "species"] = accepted_alt.get("species")
                dat.loc[ind_tax, "genus"] = accepted_alt.get("genus")
                dat.loc[ind_tax, "family"] = accepted_alt.get("family")
                dat.loc[ind_tax, "class"] = accepted_alt.get("class")
                dat.loc[ind_tax, "order"] = accepted_alt.get("order")
                dat.loc[ind_tax, "phylum"] = accepted_alt.get("phylum")
                dat.loc[ind_tax, "kingdom"] = accepted_alt.get("kingdom")
                dat.loc[ind_tax, "GBIFstatus_Synonym"] = "ACCEPTED"
                dat.loc[ind_tax, "usageKey"] = accepted_alt.get("usageKey")
                dat.loc[ind_tax, "GBIFstatus"] = "ACCEPTED"
                dat.loc[ind_tax, "note"] = "Synonym with accepted alt"
                continue

            elif db.get("rank") == "SPECIES":
                dat.loc[ind_tax, "Taxon"] = db.get("species")
                dat.loc[ind_tax, "GBIFstatus"] = db.get("status")
                dat.loc[ind_tax, "GBIFmatchtype"] = db.get("matchType")
                dat.loc[ind_tax, "GBIFtaxonRank"] = db.get("rank")
                dat.loc[ind_tax, "GBIFusageKey"] = db.get("usageKey")
                dat.loc[ind_tax, "note"] = "Synonym with no accepted alt, species rank"

                try:
                    db_all_2 = get_species_name_backbone(
                        dat.loc[ind_tax, "Taxon"].iloc[0], strict=True
                    )
                except (HTTPError, Timeout):
                    print(
                        f"Failed to retrieve data for {dat.loc[ind_tax, 'Taxon'].iloc[0]} after 5 attempts. Skipping."
                    )
                    continue

                db_2 = db_all_2

                if db_2.get("matchType") == "EXACT":
                    dat.loc[ind_tax, "scientificName"] = db_2.get("scientificName")
                    dat.loc[ind_tax, "GBIFstatus_Synonym"] = db_2.get("status")
                    dat.loc[ind_tax, "species"] = db_2.get("species")
                    dat.loc[ind_tax, "genus"] = db_2.get("genus")
                    dat.loc[ind_tax, "family"] = db_2.get("family")
                    dat.loc[ind_tax, "class"] = db_2.get("class")
                    dat.loc[ind_tax, "order"] = db_2.get("order")
                    dat.loc[ind_tax, "phylum"] = db_2.get("phylum")
                    dat.loc[ind_tax, "kingdom"] = db_2.get("kingdom")
                    dat.loc[ind_tax, "note"] = (
                        "Synonym with no accepted alt, species rank, exact match"
                    )
            elif db.get("rank") == "GENUS":
                dat.loc[ind_tax, "Taxon"] = db.get("genus")
                dat.loc[ind_tax, "GBIFstatus"] = db.get("status")
                dat.loc[ind_tax, "GBIFmatchtype"] = db.get("matchType")
                dat.loc[ind_tax, "GBIFtaxonRank"] = db.get("rank")
                dat.loc[ind_tax, "GBIFusageKey"] = db.get("usageKey")
                dat.loc[ind_tax, "note"] = "Synonym with no accepted alt, genus rank"
                try:
                    db_all_2 = get_species_name_backbone(
                        dat.loc[ind_tax, "Taxon"].iloc[0], strict=True
                    )
                except (HTTPError, Timeout):
                    print(
                        f"Failed to retrieve data for {dat.loc[ind_tax, 'Taxon'].iloc[0]} after 5 attempts. Skipping."
                    )
                    continue
        elif db.get("status") == "DOUBTFUL" and db.get("matchType") == "EXACT":
            dat.loc[ind_tax, "GBIFstatus"] = db.get("status")
            dat.loc[ind_tax, "GBIFmatchtype"] = db.get("matchType")
            dat.loc[ind_tax, "GBIFtaxonRank"] = db.get("rank")
            dat.loc[ind_tax, "GBIFusageKey"] = db.get("usageKey")
            dat.loc[ind_tax, "note"] = "Doubtful record"

            # Try again by stripping author name
            try:
                db_all_2 = get_species_name_backbone(
                    strip_author_name(taxon), strict=True
                )
            except (HTTPError, Timeout):
                print(
                    f"Failed to retrieve data for {strip_author_name(taxon)} after 5 attempts. Skipping."
                )
                continue

            db_2 = db_all_2

            if db_2.get("matchType") == "EXACT":
                dat.loc[ind_tax, "scientificName"] = db_2.get("scientificName")
                dat.loc[ind_tax, "GBIFstatus_Synonym"] = db_2.get("status")
                dat.loc[ind_tax, "species"] = db_2.get("species")
                dat.loc[ind_tax, "genus"] = db_2.get("genus")
                dat.loc[ind_tax, "family"] = db_2.get("family")
                dat.loc[ind_tax, "class"] = db_2.get("class")
                dat.loc[ind_tax, "order"] = db_2.get("order")
                dat.loc[ind_tax, "phylum"] = db_2.get("phylum")
                dat.loc[ind_tax, "kingdom"] = db_2.get("kingdom")
                dat.loc[ind_tax, "note"] = (
                    "Doubtful record, exact match after stripping author name"
                )
            continue
        elif (
            len(taxon.split()) > 2
        ):  # when we refactor, the code below should get replaced with a recursive call to the functio96
            taxon_binom = " ".join(taxon.split()[:2])
            print(taxon_binom)
            try:
                db_binom = get_species_name_backbone(taxon_binom, strict=True)
            except (HTTPError, Timeout):
                print(
                    f"Failed to retrieve data for {taxon_binom} after 5 attempts. Skipping."
                )
                continue

            db_2 = db_binom

            if db_2.get("matchType") == "EXACT" and db_2.get("status") == "ACCEPTED":
                dat.loc[ind_tax, "scientificName"] = db_2.get("scientificName")
                dat.loc[ind_tax, "Taxon"] = db_2.get("canonicalName")
                dat.loc[ind_tax, "GBIFstatus"] = db_2.get("status")
                dat.loc[ind_tax, "GBIFmatchtype"] = db_2.get("matchType")
                dat.loc[ind_tax, "GBIFtaxonRank"] = db_2.get("rank")
                dat.loc[ind_tax, "GBIFusageKey"] = db_2.get("usageKey")

                dat.loc[ind_tax, "species"] = db_2.get("species")
                dat.loc[ind_tax, "genus"] = db_2.get("genus")
                dat.loc[ind_tax, "family"] = db_2.get("family")
                dat.loc[ind_tax, "class"] = db_2.get("class")
                dat.loc[ind_tax, "order"] = db_2.get("order")
                dat.loc[ind_tax, "phylum"] = db_2.get("phylum")
                dat.loc[ind_tax, "kingdom"] = db_2.get("kingdom")
                dat.loc[ind_tax, "note"] = "Exact match after splitting binomial name"
            elif (
                db_2.get("status") == "SYNONYM"
                and db_2.get("matchType") == "EXACT"
                and ("species" in db_2 or "genus" in db_2)
            ):
                try:
                    accepted_db = species.name_usage(key=db_2.get("acceptedUsageKey"))
                except (HTTPError, Timeout):
                    print(
                        f"Failed to retrieve data for {db_2.get('acceptedUsageKey')} after 5 attempts. Skipping."
                    )
                    continue
                if accepted_db.get("taxonomicStatus") == "ACCEPTED":
                    dat.loc[ind_tax, "scientificName"] = accepted_db.get("scientificName")
                    dat.loc[ind_tax, "Taxon"] = accepted_db.get("canonicalName")
                    dat.loc[ind_tax, "GBIFstatus"] = accepted_db.get("taxonomicStatus")
                    dat.loc[ind_tax, "GBIFmatchtype"] = db_2.get("matchType")
                    dat.loc[ind_tax, "GBIFtaxonRank"] = db_2.get("rank")
                    dat.loc[ind_tax, "GBIFusageKey"] = db_2.get("usageKey")

                    dat.loc[ind_tax, "species"] = accepted_db.get("species")
                    dat.loc[ind_tax, "genus"] = accepted_db.get("genus")
                    dat.loc[ind_tax, "family"] = accepted_db.get("family")
                    dat.loc[ind_tax, "class"] = accepted_db.get("class")
                    dat.loc[ind_tax, "order"] = accepted_db.get("order")
                    dat.loc[ind_tax, "phylum"] = accepted_db.get("phylum")
                    dat.loc[ind_tax, "kingdom"] = accepted_db.get("kingdom")
                    dat.loc[ind_tax, "note"] = "Synonym with accepted alt after splitting binomial name"
        else:
            dat.loc[ind_tax, "note"] = "No match found"
            mismatch_entry = {
                "Taxon": taxon,
                "status": db.get("status"),
                "matchType": db.get("matchType"),
            }
            mismatches = pd.concat(
                [mismatches, pd.DataFrame([mismatch_entry])], ignore_index=True
            )

    return dat, mismatches


def update_GBIFstatus(row):
    if row["GBIFstatus"] == "Missing" and row["GBIFstatus_Synonym"] != None:
        row["GBIFstatus"] = row["GBIFstatus_Synonym"]
    elif row["GBIFstatus"] == None and row["GBIFstatus_Synonym"] != None:
        row["GBIFstatus"] = row["GBIFstatus_Synonym"]
    return row
