#' GIATAR Query Functions
#'
#' This script contains functions to facilitate querying the GIATAR database.
#' The functions allow users to retrieve species information, introduction records,
#' ecology data, host and vector information, and more.

if (!requireNamespace("dplyr", quietly = TRUE)) install.packages("dplyr")
if (!requireNamespace("readr", quietly = TRUE)) install.packages("readr")
if (!requireNamespace("dotenv", quietly = TRUE)) install.packages("dotenv")
if (!requireNamespace("httr", quietly = TRUE)) install.packages("httr")

library(dplyr)
library(readr)
library(dotenv)
library(httr)

if (!length(Sys.getenv("DATA_PATH")) | Sys.getenv("DATA_PATH") == "") {
  # Prompt the user to enter GIATAR_path
  cat("DATA_PATH environment variable not found.\n")
  GIATAR_path <- readline(prompt = "Enter DATA_PATH: ")

  # Set the entered value as an environment variable
  Sys.setenv(DATA_PATH = DATA_PATH)
} else {
  # Retrieve the value of GIATAR_path environment variable
  DATA_PATH <- Sys.getenv("DATA_PATH")
}

setwd(DATA_PATH)

invasive_all_source <- read_csv("species lists/invasive_all_source.csv", col_types = cols(usageKey = col_character()))
first_records <- read_csv("occurrences/first_records.csv", col_types = cols(usageKey = col_character()), lazy = FALSE)
all_records <- read_csv("occurrences/all_records.csv", col_types = cols(usageKey = col_character()), lazy = FALSE)
native_ranges <- read_csv("native ranges/all_sources_native_ranges.csv", col_types = cols(usageKey = col_character()), lazy = FALSE)
native_range_crosswalk <- read_csv("native ranges/native_range_crosswalk.csv", lazy = FALSE)
CABI_rainfall <- read_csv("CABI data/CABI_tables/torainfall.csv", col_types = cols(usageKey = col_character()))
CABI_airtemp <- read_csv("CABI data/CABI_tables/toairTemperature.csv", col_types = cols(usageKey = col_character()))
CABI_climate <- read_csv("CABI data/CABI_tables/toclimate.csv", col_types = cols(usageKey = col_character()))
CABI_environments <- read_csv("CABI data/CABI_tables/toenvironments.csv", col_types = cols(usageKey = col_character()))
CABI_lat_alt <- read_csv("CABI data/CABI_tables/tolatitudeAndAltitudeRanges.csv", col_types = cols(usageKey = col_character()))
CABI_natural_enemies <- read_csv("CABI data/CABI_tables/tonaturalEnemies.csv", col_types = cols(usageKey = col_character()))
CABI_water_tolerances <- read_csv("CABI data/CABI_tables/towaterTolerances.csv", col_types = cols(usageKey = col_character()))
CABI_wood_packaging <- read_csv("CABI data/CABI_tables/towoodPackaging.csv", col_types = cols(usageKey = col_character()))
DAISIE_habitats <- read_csv("DAISIE data/DAISIE_habitat.csv", col_types = cols(usageKey = col_character()))
CABI_tohostPlants <- read_csv("CABI data/CABI_tables/tohostPlants.csv", col_types = cols(usageKey = col_character()))
CABI_topathwayVectors <- read_csv("CABI data/CABI_tables/topathwayVectors.csv", col_types = cols(usageKey = col_character()))
CABI_topathwayCauses <- read_csv("CABI data/CABI_tables/topathwayCauses.csv", col_types = cols(usageKey = col_character()))
CABI_toplantTrade <- read_csv("CABI data/CABI_tables/toplantTrade.csv", col_types = cols(usageKey = col_character()))
CABI_tovectorsAndIntermediateHosts <- read_csv("CABI data/CABI_tables/tovectorsAndIntermediateHosts.csv", col_types = cols(usageKey = col_character()))
EPPO_hosts <- read_csv("EPPO data/EPPO_hosts.csv", col_types = cols(usageKey = col_character()))
DAISIE_pathways <- read_csv("DAISIE data/DAISIE_pathways.csv", col_types = cols(usageKey = col_character()))
DAISIE_vectors <- read_csv("DAISIE data/DAISIE_vectors.csv", col_types = cols(usageKey = col_character()))
GBIF_backbone_invasive <- read_csv("GBIF data/GBIF_backbone_invasive.csv")
DAISIE_vernacular <- read_csv("DAISIE data/DAISIE_vernacular_names.csv", col_types = cols(usageKey = col_character()), lazy = FALSE)
EPPO_names <- read_csv("EPPO data/EPPO_names.csv", col_types = cols(usageKey = col_character()), lazy = FALSE)

#' Get Species Name from Usage Key
#'
#' This function retrieves the species name corresponding to a given usage key from the invasive_all_source dataset.
#'
#' @param usageKey A character string specifying the usage key for which to retrieve the species name.
#'
#' @return A character string containing the species name corresponding to the given usage key.
#' @export
#'
#' @examples
#' \dontrun{
#'   species_name <- get_species_name("4528099")
#'   print(species_name)
#' }
get_species_name <- function(usageKey) {
  if (!is.character(usageKey)) {
    usageKey <- gsub(".0", "", as.character(usageKey))
  }
  if (usageKey %in% invasive_all_source$usageKey) {
    return(dplyr::filter(invasive_all_source, usageKey == usageKey)$canonicalName)
  }
}

#' Get Usage Key for a Species
#'
#' This function retrieves the usage key corresponding to a given species name from the invasive_all_source dataset.
#'
#' @param species_name A string specifying the name of the species for which to retrieve the usage key.
#'
#' @return A character string containing the usage key corresponding to the given species name.
#' @export
#'
#' @examples
#' \dontrun{
#'   usage_key <- get_usageKey("Ailanthus altissima")
#'   print(usage_key)
#' }
get_usageKey <- function(species_name) {
  if (!exists("invasive_all_source")) {
    invasive_all_source <- read.csv("species lists\\invasive_all_source.csv")
  }
  if ("canonicalName" %in% colnames(invasive_all_source) && species_name %in% invasive_all_source$canonicalName) {
    return(dplyr::filter(invasive_all_source, canonicalName == species_name)$usageKey[1])
  } else if ("taxonSINAS" %in% colnames(invasive_all_source) && species_name %in% invasive_all_source$taxonSINAS) {
    return(dplyr::filter(invasive_all_source, taxonSINAS == species_name)$usageKey[1])
  } else if ("taxonEPPO" %in% colnames(invasive_all_source) && species_name %in% invasive_all_source$taxonEPPO) {
    return(dplyr::filter(invasive_all_source, taxonEPPO == species_name)$usageKey[1])
  } else if ("taxonCABI" %in% colnames(invasive_all_source) && species_name %in% invasive_all_source$taxonCABI) {
    return(dplyr::filter(invasive_all_source, taxonCABI == species_name)$usageKey[1])
  } else if ("usageKey" %in% colnames(invasive_all_source) && species_name %in% invasive_all_source$usageKey) {
    return(species_name)
  } else if ("canonicalName" %in% colnames(invasive_all_source) && species_name %in% invasive_all_source$canonicalName) {
    return(dplyr::filter(invasive_all_source, speciesGBIF == species_name)$usageKey[1])
  } else if ("taxonDAISIE" %in% colnames(invasive_all_source) && species_name %in% invasive_all_source$taxonDAISIE) {
    return(dplyr::filter(invasive_all_source, taxonDAISIE == species_name)$usageKey[1])
  } else if (grepl("^[xX]{2}", species_name) || grepl("^\\d+$", species_name)) {
    return(species_name)
  } else {
    gbif <- rgbif::name_backbone(name = species_name, rank = "species")

    if ("NONE" %in% gbif$matchType) {
      cat("species not found in Database or GBIF\n")
      return(NULL)
    } else {
      return(as.character(gbif$usageKey))
    }
  }
}

#' Get All Species
#'
#' This function retrieves a list of all species names from the invasive_all_source dataset.
#'
#' @return A character vector containing the names of all species in the invasive_all_source dataset.
#' @export
#'
#' @examples
#' \dontrun{
#'   species_list <- get_all_species()
#'   print(species_list)
#' }
get_all_species <- function() {
  species_list <- vector()
  for (i in 1:nrow(invasive_all_source)) {
    if (invasive_all_source$rank[i] %in% c("SPECIES", "FORM", "SUBSPECIES", "VARIETY")) {
      species_list <- append(species_list, invasive_all_source$canonicalName[i])
    } else if (!is.na(invasive_all_source$taxonEPPO[i])) {
      species_list <- append(species_list, invasive_all_source$taxonEPPO[i])
    } else if (!is.na(invasive_all_source$taxonSINAS[i])) {
      if (invasive_all_source$taxonSINAS[i] %in% species_list) {
        species_list <- append(species_list, invasive_all_source$taxonCABI[i])
      } else {
        species_list <- append(species_list, invasive_all_source$taxonSINAS[i])
      }
    } else if (!is.na(invasive_all_source$taxonCABI[i])) {
      species_list <- append(species_list, invasive_all_source$taxonCABI[i])
    }
  }
  return(species_list)
}

#' Check if a Species Exists in the Database
#'
#' This function checks if a given species exists in the invasive_all_source dataset.
#'
#' @param species_name A string specifying the name of the species to check.
#'
#' @return A boolean value indicating whether the species exists in the database (TRUE) or not (FALSE).
#' @export
#'
#' @examples
#' \dontrun{
#'   exists <- check_species_exists("Ailanthus altissima")
#'   print(exists)
#' }
check_species_exists <- function(species_name) {
  usage_key <- get_usageKey(species_name)

  if (!is.null(usage_key) && (usage_key %in% invasive_all_source$usageKey)) {
    return(TRUE)
  } else {
    return(FALSE)
  }
}

#' Get First Introductions for a Species
#'
#' This function retrieves the first introduction records for a given species from the GIATAR database.
#' It can optionally check if the species exists in the database, filter the results to only include ISO3 codes,
#' and import additional native range information.
#'
#' @param species_name A string specifying the name of the species for which to retrieve the first introduction records.
#' @param check_exists A boolean indicating whether to check if the species exists in the database. Defaults to FALSE.
#' @param ISO3_only A boolean indicating whether to return only records with 3-character ISO3 codes. Defaults to FALSE.
#' @param import_additional_native_info A boolean indicating whether to import additional native range information. Defaults to TRUE.
#'
#' @return A data frame containing the first introduction records for the specified species.
#' @export
#'
#' @examples
#' \dontrun{
#'   first_introductions <- get_first_introductions("Ailanthus altissima")
#'   print(first_introductions)
#' }
get_first_introductions <- function(species_name, check_exists = FALSE, ISO3_only = FALSE, import_additional_native_info = TRUE) {
  usageKey <- get_usageKey(species_name)
  if (check_exists == TRUE) {
    if (!check_species_exists(usageKey)) {
      stop("Species not in Database. Try checking master list with get_all_species()")
    }
  }
  df <- first_records %>% dplyr::filter(usageKey == !!usageKey)
  if (ISO3_only == TRUE) {
    df <- df %>% dplyr::filter(nchar(ISO3) == 3)
  }
  if (import_additional_native_info == TRUE) {
    ISO3_list <- unique(as.character(df$ISO3))

    # Filter out values with more than 3 characters
    ISO3_list <- ISO3_list[nchar(ISO3_list) == 3]
    native_ranges <- get_native_ranges(usageKey, ISO3 = unique(ISO3_list))

    native_ranges <- native_ranges %>% dplyr::filter(!is.na(Native))
    for (i in 1:nrow(native_ranges)) {
      if (native_ranges$Native[i] == TRUE) {
        df <- df %>% dplyr::mutate(Native = ifelse(ISO3 == native_ranges$ISO3[i], TRUE, Native))
      } else {
        df <- df %>% dplyr::mutate(Native = ifelse(ISO3 == native_ranges$ISO3[i], FALSE, Native))
      }
    }
    return(df %>% dplyr::filter(!ISO3 %in% c("ZZ", "XL", "XZ")))
  } else {
    return(df %>% dplyr::filter(!ISO3 %in% c("ZZ", "XL", "XZ")))
  }
}

#' Get All Introductions for a Species
#'
#' This function retrieves all introduction records for a given species from the GIATAR database.
#' It can optionally check if the species exists in the database, filter the results to only include ISO3 codes,
#' and import additional native range information.
#'
#' @param species_name A string specifying the name of the species for which to retrieve all introduction records.
#' @param check_exists A boolean indicating whether to check if the species exists in the database. Defaults to FALSE.
#' @param ISO3_only A boolean indicating whether to return only records with 3-character ISO3 codes. Defaults to TRUE.
#' @param import_additional_native_info A boolean indicating whether to import additional native range information. Defaults to TRUE.
#'
#' @return A data frame containing all introduction records for the specified species.
#' @export
#'
#' @examples
#' \dontrun{
#'   all_introductions <- get_all_introductions("Ailanthus altissima")
#'   print(all_introductions)
#' }
get_all_introductions <- function(species_name, check_exists = FALSE, ISO3_only = TRUE, import_additional_native_info = TRUE) {
  usageKey <- get_usageKey(species_name)
  if (check_exists == TRUE) {
    if (!check_species_exists(usageKey)) {
      stop("Species not in Database. Try checking master list with get_all_species()")
    }
  }
  if (is.numeric(usageKey)) {
    usageKey <- as.character(usageKey)
  }
  if (!is.numeric(usageKey)) {
    usageKey <- get_usageKey(usageKey)
  }

  df <- all_records %>% dplyr::filter(usageKey == !!usageKey)
  if (ISO3_only == TRUE) {
    df <- df %>% dplyr::filter(nchar(ISO3) == 3)
  }

  if (import_additional_native_info == TRUE) {
    ISO3_list <- unique(as.character(df$ISO3))
    ISO3_list <- ISO3_list[nchar(ISO3_list) == 3]
    native_ranges <- get_native_ranges(usageKey, ISO3 = unique(ISO3_list))
    native_ranges <- native_ranges %>% dplyr::filter(!is.na(Native))
    for (i in 1:nrow(native_ranges)) {
      if (native_ranges$Native[i] == TRUE) {
        df <- df %>% dplyr::mutate(Native = ifelse(ISO3 == native_ranges$ISO3[i], TRUE, Native))
      } else {
        df <- df %>% dplyr::mutate(Native = ifelse(ISO3 == native_ranges$ISO3[i], FALSE, Native))
      }
    }
    return(df %>% dplyr::filter(!ISO3 %in% c("ZZ", "XL", "XZ")))
  } else {
    return(df %>% dplyr::filter(!ISO3 %in% c("ZZ", "XL", "XZ")))
  }
}

#' Get Ecology Information for a Species
#'
#' This function retrieves the ecology information for a given species from various databases.
#'
#' @param species_name A string specifying the name of the species for which to retrieve ecology information.
#' @param check_exists A boolean indicating whether to check if the species exists in the database. Defaults to FALSE.
#'
#' @return A list containing data frames of ecology information from various databases.
#' @export
#'
#' @examples
#' \dontrun{
#'   ecology_info <- get_ecology("Ailanthus altissima")
#'   print(ecology_info)
#' }
get_ecology <- function(species_name, check_exists = FALSE) {
  if (check_exists == TRUE) {
    if (!check_species_exists(species_name)) {
      stop("Species not in Database. Try checking master list with get_all_species()")
    }
  }

  result_dict <- list()
  usageKey <- get_usageKey(species_name)

  result_dict$CABI_rainfall <- CABI_rainfall %>% dplyr::filter(usageKey == !!usageKey)
  result_dict$CABI_airtemp <- CABI_airtemp %>% dplyr::filter(usageKey == !!usageKey)
  result_dict$CABI_climate <- CABI_climate %>% dplyr::filter(usageKey == !!usageKey)
  result_dict$CABI_environments <- CABI_environments %>% dplyr::filter(usageKey == !!usageKey)
  result_dict$CABI_lat_alt <- CABI_lat_alt %>% dplyr::filter(usageKey == !!usageKey)
  result_dict$CABI_water_tolerances <- CABI_water_tolerances %>% dplyr::filter(usageKey == !!usageKey)
  result_dict$CABI_wood_packaging <- CABI_wood_packaging %>% dplyr::filter(usageKey == !!usageKey)
  result_dict$DAISIE_habitats <- DAISIE_habitats %>% dplyr::filter(usageKey == !!usageKey)

  # Remove rows with all NA from each table
  result_dict <- lapply(result_dict, function(x) {
    x <- x %>% dplyr::filter(!apply(is.na(.) | . == "", 1, all))
    if (nrow(x) == 0) {
      return(NULL)
    } else {
      return(x)
    }
  })

  # Check if all tables returned no data
  if (all(sapply(result_dict, is.null))) {
    cat("No data found for the species.\n")
    return(NULL)
  }

  return(result_dict)
}

#' Get Hosts and Vectors for a Species
#'
#' This function retrieves the hosts and vectors associated with a given species from various databases.
#'
#' @param species_name A string specifying the name of the species for which to retrieve hosts and vectors.
#' @param check_exists A boolean indicating whether to check if the species exists in the database. Defaults to FALSE.
#'
#' @return A list containing data frames of hosts and vectors from various databases.
#' @export
#'
#' @examples
#' \dontrun{
#'   hosts_and_vectors <- get_hosts_and_vectors("Ailanthus altissima")
#'   print(hosts_and_vectors)
#' }
get_hosts_and_vectors <- function(species_name, check_exists = FALSE) {
  if (check_exists == TRUE) {
    if (!check_species_exists(species_name)) {
      stop("Species not in Database. Try checking master list with get_all_species()")
    }
  }

  usageKey <- get_usageKey(species_name)

  results_dict <- list()
  results_dict$CABI_tohostPlants <- CABI_tohostPlants %>% dplyr::filter(usageKey == !!usageKey)
  results_dict$CABI_topathwayVectors <- CABI_topathwayVectors %>% dplyr::filter(usageKey == !!usageKey)
  results_dict$CABI_topathwayCauses <- CABI_topathwayCauses %>% dplyr::filter(usageKey == !!usageKey)
  results_dict$CABI_tovectorsAndIntermediateHosts <- CABI_tovectorsAndIntermediateHosts %>% dplyr::filter(usageKey == !!usageKey)
  results_dict$EPPO_hosts <- EPPO_hosts %>% dplyr::filter(usageKey == !!usageKey)
  results_dict$DAISIE_pathways <- DAISIE_pathways %>% dplyr::filter(usageKey == !!usageKey)
  results_dict$DAISIE_vectors <- DAISIE_vectors %>% dplyr::filter(usageKey == !!usageKey)
  results_dict$CABI_toplantTrade <- CABI_toplantTrade %>% dplyr::filter(usageKey == !!usageKey)

  # Remove rows with all NA from each table
  results_dict <- lapply(results_dict, function(x) {
    if (is.null(x) || nrow(x) == 0) {
      return(NULL)
    } else {
      return(x)
    }
  })

  # Check if all tables returned no data
  if (all(sapply(results_dict, is.null))) {
    cat("No data found for the species.\n")
    return(NULL)
  }

  return(results_dict)
}

#' Get Species List
#'
#' This function retrieves a list of species usageKeys based on the specified taxonomic filters.
#'
#' @param kingdom An optional string specifying the kingdom to filter by. Defaults to NULL.
#' @param phylum An optional string specifying the phylum to filter by. Defaults to NULL.
#' @param taxonomic_class An optional string specifying the class to filter by. Defaults to NULL.
#' @param order An optional string specifying the order to filter by. Defaults to NULL.
#' @param family An optional string specifying the family to filter by. Defaults to NULL.
#' @param genus An optional string specifying the genus to filter by. Defaults to NULL.
#'
#' @return A character vector containing the usageKeys of the species that match the specified filters.
#' @export
#'
#' @examples
#' \dontrun{
#'   species_list <- get_species_list(kingdom = "Plantae", family = "Fagaceae")
#'   print(species_list)
#' }
get_species_list <- function(kingdom = NULL, phylum = NULL, taxonomic_class = NULL, order = NULL, family = NULL, genus = NULL) {
  # Filter the GBIF_backbone_invasive dataset using dplyr::filter
  filtered_data <- GBIF_backbone_invasive
  
  if (!is.null(kingdom)) {
    filtered_data <- filtered_data %>% dplyr::filter(kingdom == !!kingdom)
  }
  if (!is.null(phylum)) {
    filtered_data <- filtered_data %>% dplyr::filter(phylum == !!phylum)
  }
  if (!is.null(taxonomic_class)) {
    filtered_data <- filtered_data %>% dplyr::filter(class == !!taxonomic_class)
  }
  if (!is.null(order)) {
    filtered_data <- filtered_data %>% dplyr::filter(order == !!order)
  }
  if (!is.null(family)) {
    filtered_data <- filtered_data %>% dplyr::filter(family == !!family)
  }
  if (!is.null(genus)) {
    filtered_data <- filtered_data %>% dplyr::filter(genus == !!genus)
  }
  
  usageKey_list <- unique(filtered_data$usageKey)
  return(usageKey_list)
}


#' Get Native Ranges for a Species
#'
#' This function retrieves the native ranges for a given species from the GIATAR database.
#'
#' @param species_name A string specifying the name of the species for which to retrieve native ranges.
#' @param ISO3 An optional character vector of ISO3 codes to filter the native ranges. Defaults to NULL.
#' @param check_exists A boolean indicating whether to check if the species exists in the database. Defaults to FALSE.
#'
#' @return A data frame containing the native ranges for the specified species. If `ISO3` is provided, the data frame is filtered by the ISO3 codes.
#' @export
#'
#' @examples
#' \dontrun{
#'   native_ranges <- get_native_ranges("Ailanthus altissima")
#'   print(native_ranges)
#' }
get_native_ranges <- function(species_name, ISO3 = NULL, check_exists = FALSE) {
  if (check_exists == TRUE) {
    if (!check_species_exists(species_name)) {
      stop("Species not in Database. Try checking master list with get_all_species()")
    }
  }

  usageKey <- get_usageKey(species_name)

  if (is.null(ISO3)) {
    records <- all_records %>% dplyr::filter(usageKey == !!usageKey)
    records <- records %>% dplyr::filter(!is.na(Native))
    records <- records %>% dplyr::filter(Source != "Original")
    records <- records %>% dplyr::select(ISO3, Source, Native, Reference)
    records <- records %>% dplyr::mutate(bioregion = NA, DAISIE_region = NA)

    native_ranges_temp <- native_ranges %>% dplyr::filter(usageKey == !!usageKey)
    native_ranges_temp <- native_ranges_temp %>% dplyr::select(Source, bioregion, DAISIE_region, Reference)
    native_ranges_temp <- native_ranges_temp %>% dplyr::mutate(Native = TRUE, ISO3 = NA)

    records <- dplyr::bind_rows(records, native_ranges_temp)

    return(records)
  } else if (!is.null(ISO3)) {
    ISO3 <- ISO3[!is.na(ISO3) & nchar(ISO3) == 3]

    native_ranges <- native_ranges %>% dplyr::mutate(usageKey = as.character(usageKey))

    records <- all_records %>% dplyr::filter(usageKey == !!usageKey)
    records <- records %>% dplyr::filter(!is.na(Native))
    records <- records %>% dplyr::filter(Source != "Original")
    records <- records %>% dplyr::select(ISO3, Source, Native, Reference)
    records <- records %>% dplyr::mutate(bioregion = NA, DAISIE_region = NA)

    t_f_df <- tibble::tibble(ISO3 = character(), Native = logical(), src = character())

    bioregions <- native_ranges %>% dplyr::filter(usageKey == !!usageKey) %>% dplyr::pull(bioregion) %>% unique()

    for (iso3 in ISO3) {
      native <- NA
      src <- NA

      if (length(bioregions) > 0) {
        bioregions_temp <- native_range_crosswalk %>% 
          dplyr::filter(ISO3 == !!iso3) %>% 
          dplyr::pull(modified_Bioregion) %>% 
          unique()

        for (br in bioregions_temp) {
          if (br %in% bioregions) {
            native <- TRUE
            src <- "br"
            break
          } else {
            native <- FALSE
            src <- "br"
          }
        }
      }

      if (iso3 %in% records$ISO3) {
        native <- records %>% dplyr::filter(ISO3 == !!iso3) %>% dplyr::pull(Native)
        src <- "records"
      }

      if (length(bioregions) == 0 && !(iso3 %in% records$ISO3)) {
        native <- NA
        src <- "records only - no bioregion found"
      }

      t_f_df <- dplyr::bind_rows(t_f_df, tibble::tibble(ISO3 = iso3, Native = native, src = src))
    }

    return(t_f_df)
  }
}

#' Get Common Names for a Species
#'
#' This function retrieves the common names for a given species from the DAISIE and EPPO databases.
#'
#' @param species_name A string specifying the name of the species for which to retrieve common names.
#' @param check_exists A boolean indicating whether to check if the species exists in the database. Defaults to FALSE.
#'
#' @return A list containing data frames of common names from DAISIE and EPPO databases.
#' @export
#'
#' @examples
#' \dontrun{
#'   common_names <- get_common_names("Ailanthus altissima")
#'   print(common_names)
#' }
get_common_names <- function(species_name, check_exists = FALSE) {
  if (check_exists == TRUE) {
    if (!check_species_exists(species_name)) {
      stop("Species not in Database. Try checking master list with get_all_species()")
    }
  }

  usageKey <- get_usageKey(species_name)

  # Assume DAISIE_vernacular and EPPO_names are available in the global environment

  results_dict <- list()

  if (usageKey %in% DAISIE_vernacular$usageKey) {
    results_dict$DAISIE_vernacular <- DAISIE_vernacular %>%
      dplyr::filter(usageKey == !!usageKey) %>%
      dplyr::filter(rowSums(is.na(.)) < ncol(.))
  }

  if (usageKey %in% EPPO_names$usageKey) {
    results_dict$EPPO_names <- EPPO_names %>%
      dplyr::filter(usageKey == !!usageKey) %>%
      dplyr::filter(rowSums(is.na(.)) < ncol(.))
  }

  # Remove empty elements from the results_dict
  results_dict <- results_dict[sapply(results_dict, function(x) !is.null(x) && nrow(x) > 0)]

  return(results_dict)
}


#' Get List of Trait Tablest Tables
#'
#' This function returns a list of available trait table names. a list of available trait table names.
#'
#' @return A character vector containing the names of available trait tables.or containing the names of available trait tables.
#' @export
#'
#' @examples
#' \dontrun{
#'   trait_tables <- get_trait_table_list()t_trait_table_list()
#'   print(trait_tables)
#' }
get_trait_table_list <- function() {
    trait_tables <- c(
      "CABI_rainfall",
      "CABI_airtemp",
      "CABI_climate",
      "CABI_environments",
      "CABI_lat_alt",
      "CABI_natural_enemies",
      "CABI_water_tolerances",
      "CABI_wood_packaging",
      "CABI_host_plants",
      "CABI_pathway_vectors",
      "CABI_Plant_trade",
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
      "DAISIE_vernacular"
    )
    return(trait_tables)
}

#' Get Trait Table
#'
#' This function retrieves a specific trait table based on the provided table name.
#'
#' @param table_name A character string specifying the name of the trait table to retrieve.
#' @param usageKey An optional character string specifying the usage key to filter the table by.
#' @return A data frame containing the requested trait table. If `usageKey` is provided, the table is filtered by the usage key.
#' @details The function checks if the provided table name exists in the list of available trait tables. If it does, it constructs the file path to the corresponding CSV file and reads the data into a data frame. If a `usageKey` is provided, the function filters the data frame to include only rows with the matching usage key.
#' @examples
#' \dontrun{
#'   # Retrieve the CABI rainfall table
#'   rainfall_table <- get_trait_table("CABI_rainfall")
#'
#'   # Retrieve the CABI rainfall table filtered by a specific usage key
#'   rainfall_table_filtered <- get_trait_table("CABI_rainfall", usageKey = "12345")
#' }
#' @export
get_trait_table <- function(table_name, usageKey = NULL) {
  if (table_name %in% get_trait_table_list()) {
    file_path <- switch(table_name,
      CABI_rainfall = "CABI data/CABI_tables/torainfall.csv",
      CABI_airtemp = "CABI data/CABI_tables/toairTemperature.csv",
      CABI_climate = "CABI data/CABI_tables/toclimate.csv",
      CABI_environments = "CABI data/CABI_tables/toenvironments.csv",
      CABI_lat_alt = "CABI data/CABI_tables/tolatitudeAndAltitudeRanges.csv",
      CABI_natural_enemies = "CABI data/CABI_tables/tonaturalEnemies.csv",
      CABI_water_tolerances = "CABI data/CABI_tables/towaterTolerances.csv",
      CABI_wood_packaging = "CABI data/CABI_tables/towoodPackaging.csv",
      CABI_host_plants = "CABI data/CABI_tables/tohostPlants.csv",
      CABI_pathway_vectors = "CABI data/CABI_tables/topathwayVectors.csv",
      CABI_pathway_causes = "CABI data/CABI_tables/topathwayCauses.csv",
      CABI_vectorsAndIntermediateHosts = "CABI data/CABI_tables/tovectorsAndIntermediateHosts.csv",
      DAISIE_habitats = "DAISIE data/DAISIE_habitat.csv",
      CABI_impact_summary = "CABI data/CABI_tables/toimpactSummary.csv",
      CABI_latitude_altitude_ranges = "CABI data/CABI_tables/tolatitudeAltitudeRanges.csv",
      CABI_symptoms_signs = "CABI data/CABI_tables/tosymptomsSigns.csv",
      CABI_threatened_species = "CABI data/CABI_tables/tothreatenedSpecies.csv",
      EPPO_hosts = "EPPO data/EPPO_hosts.csv",
      EPPO_names = "EPPO data/EPPO_names.csv",
      DAISIE_pathways = "DAISIE data/DAISIE_pathways.csv",
      DAISIE_vectors = "DAISIE data/DAISIE_vectors.csv",
      DAISIE_vernacular = "DAISIE data/DAISIE_vernacular.csv"
    )
    table <- readr::read_csv(file_path, col_types = cols(usageKey = col_character()))
    if (!is.null(usageKey)) {
      table <- table[table$usageKey == usageKey, ]
    }
    return(table)
  } else {
    stop("Table name '", table_name, "' not found.")
  }
}

#' Download and Extract the Latest GIATAR Dataset
#'
#' This function downloads the latest version of the GIATAR dataset from Zenodo and extracts it to the specified directory.
#' If the files already exist, they will be overwritten.
#'
#' @param data_dir A string specifying the directory where the dataset will be extracted. The default is the current working directory.
#'
#' @return None
#' @export
#'
#' @examples
#' \dontrun{
#' get_GIATAR_current("path/to/data_directory")
#' }
get_GIATAR_current <- function(data_dir = getwd()) {
  url <- "https://zenodo.org/api/records/13138446"
  response <- httr::GET(url)
  if (httr::status_code(response) == 200) {
    data <- httr::content(response, "parsed")
    zip_url <- data$files[[1]]$links$self
    cat("Found dataset at", zip_url, ". Downloading GIATAR dataset...\n")
    zip_response <- httr::GET(zip_url)
    if (httr::status_code(zip_response) == 200) {
      temp <- tempfile()
      writeBin(httr::content(zip_response, "raw"), temp)
      unzip(temp, exdir = data_dir)
      unlink(temp)
      cat("GIATAR dataset downloaded and extracted successfully.\n")
    } else {
      cat("Failed to download GIATAR zip file. Status code:", httr::status_code(zip_response), "\n")
    }
  } else {
    cat("Failed to retrieve GIATAR dataset information. Status code:", httr::status_code(response), "\n")
  }
}

#' Retrieve Taxa Associated with a Given Host
#'
#' This function retrieves a list of taxa that are associated with a given host.
#'
#' @param host_name A string specifying the name of the host to query. This should be the host taxa's partial or full scientific name.
#'
#' @return A list of taxon names (canonical name) for taxa associated with matches for the specified host name.
#' @export
#'
#' @examples
#' \dontrun{
#' get_taxa_by_host("mays")
#' }
get_taxa_by_host <- function(host_name) {
  CABI_hosts <- readr::read_csv("CABI data/CABI_tables/tohostPlants.csv", col_types = readr::cols(usageKey = readr::col_character()))
  EPPO_hosts <- readr::read_csv("EPPO data/EPPO_hosts.csv", col_types = readr::cols(usageKey = readr::col_character()))

  # Filter the dataframes to get rows where the host name matches
  cabi_hosts <- dplyr::filter(CABI_hosts, grepl(host_name, `Plant name`, ignore.case = TRUE))
  eppo_hosts <- dplyr::filter(EPPO_hosts, grepl(host_name, full_name, ignore.case = TRUE))

  # Print all matched host names if either cabi_hosts or eppo_hosts has more than one match
  if (nrow(cabi_hosts) > 1 || nrow(eppo_hosts) > 1) {
    cat(sprintf("Host name '%s' matched the following host species:\n", host_name))
    if (nrow(cabi_hosts) > 1) {
      cat("CABI:\n")
      cat(paste(unique(cabi_hosts$`Plant name`), collapse = ", "), "\n")
    }
    if (nrow(eppo_hosts) > 1) {
      cat("EPPO:\n")
      cat(paste(unique(eppo_hosts$full_name), collapse = ", "), "\n")
    }
  }

  # Combine the results and get unique taxa
  combined_hosts <- dplyr::bind_rows(cabi_hosts, eppo_hosts)
  taxa_keys <- unique(combined_hosts$usageKey)
  # Get the canonicalNames associated with these usageKeys
  taxa_list <- invasive_all_source[invasive_all_source$usageKey %in% taxa_keys, "canonicalName"]
  

  # Print the length of the combined list
  cat(sprintf("Total number of invasive taxa associated with '%s': %d\n", host_name, length(taxa_list)))

  if (length(taxa_list) == 0) {
    cat(sprintf("No host species found for '%s'\n", host_name))
  }

  return(taxa_list)
}

#' Retrieve Taxa Associated with a Given Pathway
#'
#' This function retrieves a list of taxa that are associated with a given pathway.
#'
#' @param pathway_name A string specifying the name of the pathway to query. This should be the pathway's partial or full name.
#'
#' @return A list of taxon names (canonical name) for taxa associated with matches for the specified pathway name.
#' @export
#'
#' @examples
#' \dontrun{
#'   taxa_list <- get_taxa_by_pathway("ornamental")
#'   print(taxa_list)
#' }
get_taxa_by_pathway <- function(pathway_name) {
    CABI_pathways <- readr::read_csv("CABI data/CABI_tables/topathwayVectors.csv", col_types = readr::cols(usageKey = readr::col_character()))
    DAISIE_pathways <- readr::read_csv("DAISIE data/DAISIE_pathways.csv", col_types = readr::cols(usageKey = readr::col_character()))
    CABI_pathway_causes <- readr::read_csv("CABI data/CABI_tables/topathwayCauses.csv", col_types = readr::cols(usageKey = readr::col_character()))

    # Combine the columns "Vector" and "Notes" to create "pathway" in cabi_pathways
    CABI_pathways$pathway <- paste(CABI_pathways$Vector, CABI_pathways$Notes, sep = ": ")

    # Combine the columns "Cause" and "Notes" to create "pathway" in cabi_pathway_causes
    CABI_pathway_causes$pathway <- paste(CABI_pathway_causes$Cause, CABI_pathway_causes$Notes, sep = ": ")

    # Filter the dataframes to get rows where the pathway name matches
    cabi_pathways <- subset(CABI_pathways, grepl(pathway_name, pathway, ignore.case = TRUE))
    daisie_pathways <- subset(DAISIE_pathways, grepl(pathway_name, pathway, ignore.case = TRUE))
    cabi_pathway_causes <- subset(CABI_pathway_causes, grepl(pathway_name, pathway, ignore.case = TRUE))

    # Print all matched pathway names if any dataframe has more than one match
    if (nrow(cabi_pathways) > 1 || nrow(daisie_pathways) > 1 || nrow(cabi_pathway_causes) > 1) {
        cat(sprintf("Pathway name '%s' matched the following pathways:\n", pathway_name))
        if (nrow(cabi_pathways) > 1) {
            cat("CABI Pathways:\n")
            cat(paste(unique(cabi_pathways$pathway), collapse = ", "), "\n")
        }
        if (nrow(daisie_pathways) > 1) {
            cat("DAISIE Pathways:\n")
            cat(paste(unique(daisie_pathways$pathway), collapse = ", "), "\n")
        }
        if (nrow(cabi_pathway_causes) > 1) {
            cat("CABI Pathway Causes:\n")
            cat(paste(unique(cabi_pathway_causes$pathway), collapse = ", "), "\n")
        }

        # Combine the results and get unique taxa
        combined_pathways <- rbind(cabi_pathways, daisie_pathways, cabi_pathway_causes)
        taxa_keys <- unique(combined_pathways$usageKey)
        # Get the canonicalNames associated with these usageKeys
        taxa_list <- invasive_all_source[invasive_all_source$usageKey %in% taxa_keys, "canonicalName"]

        # Print the length of the combined list
        cat(sprintf("Total number of invasive taxa associated with '%s': %d\n", pathway_name, length(taxa_list)))
    } else {
        cat(sprintf("No pathways found for '%s'\n", pathway_name))
        taxa_list <- list()
    }
    return(taxa_list)
}

#' Retrieve Taxa Associated with a Given Vector
#'
#' This function retrieves a list of taxa that are associated with a given vector.
#'
#' @param vector_name A string specifying the name of the vector to query. This should be the vector species' partial or full scientific name.
#'
#' @return A list of taxon names (canonical name) for taxa associated with matches for the specified vector name.
#' @export
#'
#' @examples
#' \dontrun{
#'   taxa_list <- get_taxa_by_vector("Aedes aegypti")
#'   print(taxa_list)
#' }
get_taxa_by_vector <- function(vector_name) {
    CABI_vectors <- readr::read_csv("CABI data/CABI_tables/tovectorsAndIntermediateHosts.csv", col_types = readr::cols(usageKey = readr::col_character()))

    # Filter the dataframe to get rows where the vector name matches
    cabi_vectors <- subset(CABI_vectors, grepl(vector_name, Vector, ignore.case = TRUE))

    # Print all matched vector names if the dataframe has more than one match
    if (nrow(cabi_vectors) > 1) {
        cat(sprintf("Vector name '%s' matched the following vectors:\n", vector_name))
        cat(paste(unique(cabi_vectors$Vector), collapse = ", "), "\n")

        # Get unique taxa
        taxa_keys <- unique(cabi_vectors$usageKey)
        # Get the canonicalNames associated with these usageKeys
        taxa_list <- invasive_all_source[invasive_all_source$usageKey %in% taxa_keys, "canonicalName"]
        
        # Print the length of the list
        cat(sprintf("Total number of invasive taxa associated with '%s': %d\n", vector_name, length(taxa_list)))
    } else {
        cat(sprintf("No vectors found for '%s'\n", vector_name))
        taxa_list <- list()
    }
    return(taxa_list)
}