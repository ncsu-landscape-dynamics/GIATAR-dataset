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

if (!length(Sys.getenv("GIATAR_path")) | Sys.getenv("GIATAR_path") == "") {
  # Prompt the user to enter GIATAR_path
  cat("GIATAR_path environment variable not found.\n")
  GIATAR_path <- readline(prompt = "Enter GIATAR_path: ")

  # Set the entered value as an environment variable
  Sys.setenv(GIATAR_path = GIATAR_path)
} else {
  # Retrieve the value of GIATAR_path environment variable
  GIATAR_path <- Sys.getenv("GIATAR_path")
}

setwd(GIATAR_path)

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
    return(invasive_all_source[invasive_all_source$usageKey == usageKey, "canonicalName"])
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
    return(invasive_all_source$usageKey[invasive_all_source$canonicalName == species_name][1])
  } else if ("taxonSINAS" %in% colnames(invasive_all_source) && species_name %in% invasive_all_source$taxonSINAS) {
    return(invasive_all_source$usageKey[invasive_all_source$taxonSINAS == species_name][1])
  } else if ("taxonEPPO" %in% colnames(invasive_all_source) && species_name %in% invasive_all_source$taxonEPPO) {
    return(invasive_all_source$usageKey[invasive_all_source$taxonEPPO == species_name][1])
  } else if ("taxonCABI" %in% colnames(invasive_all_source) && species_name %in% invasive_all_source$taxonCABI) {
    return(invasive_all_source$usageKey[invasive_all_source$taxonCABI == species_name][1])
  } else if ("usageKey" %in% colnames(invasive_all_source) && species_name %in% invasive_all_source$usageKey) {
    return(species_name)
  } else if ("canonicalName" %in% colnames(invasive_all_source) && species_name %in% invasive_all_source$canonicalName) {
    return(invasive_all_source$usageKey[invasive_all_source$speciesGBIF == species_name][1])
  } else if ("taxonDAISIE" %in% colnames(invasive_all_source) && species_name %in% invasive_all_source$taxonDAISIE) {
    return(invasive_all_source$usageKey[invasive_all_source$taxonDAISIE == species_name][1])
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
  df <- first_records[first_records$usageKey == usageKey, ]
  if (ISO3_only == TRUE) {
    df <- df[nchar(df$ISO3) == 3, ]
  }
  if (import_additional_native_info == TRUE) {
    ISO3_list <- unique(as.character(df$ISO3))

    # Filter out values with more than 3 characters
    ISO3_list <- ISO3_list[nchar(ISO3_list) == 3]
    print(typeof(ISO3_list))
    native_ranges <- get_native_ranges(usageKey, ISO3 = unique(ISO3_list))

    native_ranges <- native_ranges[!is.na(native_ranges$Native), ]
    for (i in 1:nrow(native_ranges)) {
      if (native_ranges$Native[i] == TRUE) {
        df[df$ISO3 == native_ranges$ISO3[i], "Native"] <- TRUE
      } else {
        df[df$ISO3 == native_ranges$ISO3[i], "Native"] <- FALSE
      }
    }
    return(df[!df$ISO3 %in% c("ZZ", "XL", "XZ"), ])
  } else {
    return(df[!df$ISO3 %in% c("ZZ", "XL", "XZ"), ])
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

  df <- all_records[all_records$usageKey == usageKey, ]
  if (ISO3_only == TRUE) {
    df <- df[nchar(df$ISO3) == 3, ]
  }

  if (import_additional_native_info == TRUE) {
    ISO3_list <- unique(as.character(unique(df$ISO3)))
    ISO3_list <- ISO3_list[nchar(ISO3_list) == 3]
    native_ranges <- get_native_ranges(usageKey, ISO3 = unique(ISO3_list))
    native_ranges <- native_ranges[!is.na(native_ranges$Native), ]
    for (i in 1:nrow(native_ranges)) {
      if (native_ranges$Native[i] == TRUE) {
        df[df$ISO3 == native_ranges$ISO3[i], "Native"] <- TRUE
      } else {
        df[df$ISO3 == native_ranges$ISO3[i], "Native"] <- FALSE
      }
    }
    return(df[!df$ISO3 %in% c("ZZ", "XL", "XZ"), ])
  } else {
    return(df[!df$ISO3 %in% c("ZZ", "XL", "XZ"), ])
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

  result_dict$CABI_rainfall <- CABI_rainfall[CABI_rainfall$usageKey == usageKey, ]
  result_dict$CABI_airtemp <- CABI_airtemp[CABI_airtemp$usageKey == usageKey, ]
  result_dict$CABI_climate <- CABI_climate[CABI_climate$usageKey == usageKey, ]
  result_dict$CABI_environments <- CABI_environments[CABI_environments$usageKey == usageKey, ]
  result_dict$CABI_lat_alt <- CABI_lat_alt[CABI_lat_alt$usageKey == usageKey, ]
  result_dict$CABI_water_tolerances <- CABI_water_tolerances[CABI_water_tolerances$usageKey == usageKey, ]
  result_dict$CABI_wood_packaging <- CABI_wood_packaging[CABI_wood_packaging$usageKey == usageKey, ]
  result_dict$DAISIE_habitats <- DAISIE_habitats[DAISIE_habitats$usageKey == usageKey, ]

  # Remove rows with all NA from each table
  result_dict <- lapply(result_dict, function(x) {
    x <- x[!apply(is.na(x) | x == "", 1, all), ]
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
  results_dict$CABI_tohostPlants <- CABI_tohostPlants[CABI_tohostPlants$usageKey == usageKey, ]
  results_dict$CABI_topathwayVectors <- CABI_topathwayVectors[CABI_topathwayVectors$usageKey == usageKey, ]
  results_dict$CABI_topathwayCauses <- CABI_topathwayCauses[CABI_topathwayCauses$usageKey == usageKey, ]
  results_dict$CABI_tovectorsAndIntermediateHosts <- CABI_tovectorsAndIntermediateHosts[CABI_tovectorsAndIntermediateHosts$usageKey == usageKey, ]
  results_dict$EPPO_hosts <- EPPO_hosts[EPPO_hosts$usageKey == usageKey, ]
  results_dict$DAISIE_pathways <- DAISIE_pathways[DAISIE_pathways$usageKey == usageKey, ]
  results_dict$DAISIE_vectors <- DAISIE_vectors[DAISIE_vectors$usageKey == usageKey, ]
  results_dict$CABI_toplantTrade <- CABI_toplantTrade[CABI_toplantTrade$usageKey == usageKey, ]

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
  # GBIF_backbone_invasive <- read.csv("GBIF data\\GBIF_backbone_invasive.csv", stringsAsFactors = FALSE)
  if (!is.null(kingdom)) {
    GBIF_backbone_invasive <- GBIF_backbone_invasive[GBIF_backbone_invasive$kingdom == kingdom, ]
  }
  if (!is.null(phylum)) {
    GBIF_backbone_invasive <- GBIF_backbone_invasive[GBIF_backbone_invasive$phylum == phylum, ]
  }
  if (!is.null(taxonomic_class)) {
    GBIF_backbone_invasive <- GBIF_backbone_invasive[GBIF_backbone_invasive$class == taxonomic_class, ]
  }
  if (!is.null(order)) {
    GBIF_backbone_invasive <- GBIF_backbone_invasive[GBIF_backbone_invasive$order == order, ]
  }
  if (!is.null(family)) {
    GBIF_backbone_invasive <- GBIF_backbone_invasive[GBIF_backbone_invasive$family == family, ]
  }
  if (!is.null(genus)) {
    GBIF_backbone_invasive <- GBIF_backbone_invasive[GBIF_backbone_invasive$genus == genus, ]
  }
  usageKey_list <- unique(GBIF_backbone_invasive$usageKey)
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
    records <- all_records[all_records$usageKey == usageKey, ]
    print(tail(records))
    records <- records[!is.na(records$Native), ]
    print(head(records))
    records <- records[records$Source != "Original", ]
    records <- records[, c("ISO3", "Source", "Native", "Reference")]
    records$bioregion <- NA
    records$DAISIE_region <- NA

    native_ranges_temp <- native_ranges[native_ranges$usageKey == usageKey, ]
    native_ranges_temp <- native_ranges_temp[, c("Source", "bioregion", "DAISIE_region", "Reference")]
    native_ranges_temp$Native <- TRUE
    native_ranges_temp$ISO3 <- NA

    records <- rbind(records, native_ranges_temp)

    return(records)
  } else if (!is.null(ISO3)) {
    if (!is.character(ISO3) || any(sapply(ISO3, nchar) != 3)) {
      stop("ISO3 must be a list of 3 character strings")
    }

    native_ranges$usageKey <- as.character(native_ranges$usageKey)

    records <- all_records[all_records$usageKey == usageKey, ]
    records <- records[!is.na(records$Native), ]

    records <- records[records$Source != "Original", ]
    records <- records[, c("ISO3", "Source", "Native", "Reference")]
    records$bioregion <- NA
    records$DAISIE_region <- NA

    t_f_df <- data.frame(ISO3 = character(), Native = logical(), src = character(), stringsAsFactors = FALSE)

    bioregions <- unique(native_ranges[native_ranges$usageKey == usageKey, ]$bioregion)

    for (iso3 in ISO3) {
      native <- NA

      if (length(bioregions) > 0) {
        bioregions_temp <- unique(native_range_crosswalk[native_range_crosswalk$ISO3 == iso3, ]$modified_Bioregion)

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
        native <- records[records$ISO3 == iso3, ]$Native
        src <- "records"
      }

      if (length(bioregions) == 0 && !(iso3 %in% records$ISO3)) {
        native <- NA
        src <- "records only - no bioregion found"
      }

      t_f_df <- rbind(t_f_df, data.frame(ISO3 = iso3, Native = native, src = src, stringsAsFactors = FALSE))
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
    filtered_DAISIE <- DAISIE_vernacular[DAISIE_vernacular$usageKey == usageKey, ]
    results_dict$DAISIE_vernacular <- filtered_DAISIE[rowSums(is.na(filtered_DAISIE)) < ncol(filtered_DAISIE), ]
  }

  if (usageKey %in% EPPO_names$usageKey) {
    filtered_EPPO <- EPPO_names[EPPO_names$usageKey == usageKey, ]
    results_dict$EPPO_names <- filtered_EPPO[rowSums(is.na(filtered_EPPO)) < ncol(filtered_EPPO), ]
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
#' @param data_dir A string specifying the directory where the dataset will be extracted.
#'
#' @return None
#' @export
#'
#' @examples
#' \dontrun{
#' get_GIATAR_current("path/to/data_directory")
#' }
get_GIATAR_current <- function(data_dir) {
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

#' Retrieve Taxa Associated with a Given Host
#'
#' This function retrieves a list of taxa that are associated with a given host.
#'
#' @param host_name A string specifying the name of the host to query. This should be the host taxa's partial or full scientific name.
#'
#' @return A list of usageKeys associated with matches for the specified host name.
#' @export
#'
#' @examples
#' \dontrun{
#' get_taxa_by_host("Quercus")
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
  taxa_list <- unique(combined_hosts$usageKey)

  # Print the length of the combined list
  cat(sprintf("Total number of invasive taxa associated with '%s': %d\n", host_name, length(taxa_list)))

  if (length(taxa_list) == 0) {
    cat(sprintf("No host species found for '%s'\n", host_name))
  }

  return(taxa_list)
}
  return(taxa_list)
}
