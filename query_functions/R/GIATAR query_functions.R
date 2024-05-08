
## GIATAR query functions
## Converted with: https://www.codeconvert.ai/app

create_dotenv <- function(dp) {
  f <- file(".env", "w")
  writeLines(paste0("data_path=", dp), f)
  close(f)
}

if (file.exists(".env")) {
  data_path <- dotenv::get_key(".env", "data_path")
  setwd(data_path)
} else {
  print("No .env file found. Please use `create_dotenv()` to create a .env file")
  dp <- readline("Please enter the path to the data folder: ")
  create_dotenv(dp)
}


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
CABI_tovectorsAndIntermediateHosts <- read_csv("CABI data/CABI_tables/tovectorsAndIntermediateHosts.csv", col_types = cols(usageKey = col_character()))
EPPO_hosts <- read_csv("EPPO data/EPPO_hosts.csv", col_types = cols(usageKey = col_character()))
DAISIE_pathways <- read_csv("DAISIE data/DAISIE_pathways.csv", col_types = cols(usageKey = col_character()))
DAISIE_vectors <- read_csv("DAISIE data/DAISIE_vectors.csv", col_types = cols(usageKey = col_character()))
GBIF_backbone_invasive <- read_csv("GBIF data/GBIF_backbone_invasive.csv")
DAISIE_vernacular <- read_csv("DAISIE data/DAISIE_vernacular_names.csv", col_types = cols(usageKey = col_character()), lazy = FALSE)
EPPO_names <- read_csv("EPPO data/EPPO_names.csv", col_types = cols(usageKey = col_character()), lazy = FALSE)
get_species_name <- function(usageKey) {
  if (!is.character(usageKey)) {
    usageKey <- gsub(".0", "", as.character(usageKey))
  }
  if (usageKey %in% invasive_all_source$usageKey) {
    return(invasive_all_source[invasive_all_source$usageKey == usageKey, "canonicalName"])
  }
}
get_usageKey <- function(species_name) {
  if (!exists("invasive_all_source")) {
    invasive_all_source <- read.csv("species lists\\invasive_all_source.csv")
  }
  
  if ("canonicalName" %in% colnames(invasive_all_source) && species_name %in% invasive_all_source$canonicalName) {
    return(invasive_all_source$usageKey[invasive_all_source$canonicalName == species_name][1])
  } else if ("speciesASFR" %in% colnames(invasive_all_source) && species_name %in% invasive_all_source$speciesASFR) {
    return(invasive_all_source$usageKey[invasive_all_source$speciesASFR == species_name][1])
  } else if ("speciesEPPO" %in% colnames(invasive_all_source) && species_name %in% invasive_all_source$speciesEPPO) {
    return(invasive_all_source$usageKey[invasive_all_source$speciesEPPO == species_name][1])
  } else if ("speciesCABI" %in% colnames(invasive_all_source) && species_name %in% invasive_all_source$speciesCABI) {
    return(invasive_all_source$usageKey[invasive_all_source$speciesCABI == species_name][1])
  } else if ("usageKey" %in% colnames(invasive_all_source) && species_name %in% invasive_all_source$usageKey) {
    return(species_name)
  } else if ("canonicalName" %in% colnames(invasive_all_source) && species_name %in% invasive_all_source$canonicalName) {
    return(invasive_all_source$usageKey[invasive_all_source$speciesGBIF == species_name][1])
  } else if ("speciesDAISIE" %in% colnames(invasive_all_source) && species_name %in% invasive_all_source$speciesDAISIE) {
    return(invasive_all_source$usageKey[invasive_all_source$speciesDAISIE == species_name][1])
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


get_all_species <- function() {
  species_list <- vector()
  for (i in 1:nrow(invasive_all_source)) {
    if (invasive_all_source$rank[i] %in% c("SPECIES", "FORM", "SUBSPECIES", "VARIETY")) {
      species_list <- append(species_list, invasive_all_source$canonicalName[i])
    } else if (!is.na(invasive_all_source$speciesEPPO[i])) {
      species_list <- append(species_list, invasive_all_source$speciesEPPO[i])
    } else if (!is.na(invasive_all_source$speciesASFR[i])) {
      if (invasive_all_source$speciesASFR[i] %in% species_list) {
        species_list <- append(species_list, invasive_all_source$speciesCABI[i])
      } else {
        species_list <- append(species_list, invasive_all_source$speciesASFR[i])
      }
    } else if (!is.na(invasive_all_source$speciesCABI[i])) {
      species_list <- append(species_list, invasive_all_source$speciesCABI[i])
    }
  }
  return(species_list)
}

check_species_exists <- function(species_name) {
  usage_key <- get_usageKey(species_name)
  
  if (!is.null(usage_key) && (usage_key %in% invasive_all_source$usageKey)) {
    return(TRUE)
  } else {
    return(FALSE)
  }
}

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
    if (nrow(x) == 0) return(NULL) else return(x)
  })
  
  # Check if all tables returned no data
  if (all(sapply(result_dict, is.null))) {
    cat("No data found for the species.\n")
    return(NULL)
  }
  
  return(result_dict)
}
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
  print(DAISIE_pathways[DAISIE_pathways$usageKey == usageKey, ])
  results_dict$DAISIE_pathways <- DAISIE_pathways[DAISIE_pathways$usageKey == usageKey, ]
  results_dict$DAISIE_vectors <- DAISIE_vectors[DAISIE_vectors$usageKey == usageKey, ]
  
  # Remove rows with all NA from each table
  results_dict <- lapply(results_dict, function(x) {
    if (is.null(x) || nrow(x) == 0) return(NULL) else return(x)
  })
  
  # Check if all tables returned no data
  if (all(sapply(results_dict, is.null))) {
    cat("No data found for the species.\n")
    return(NULL)
  }
  
  return(results_dict)
}



get_species_list <- function(kingdom = NULL, phylum = NULL, taxonomic_class = NULL, order = NULL, family = NULL, genus = NULL) {
  #GBIF_backbone_invasive <- read.csv("GBIF data\\GBIF_backbone_invasive.csv", stringsAsFactors = FALSE)
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

get_native_ranges <- function(species_name, ISO3=NULL, check_exists=FALSE) {
  
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
    
    t_f_df <- data.frame(ISO3=character(), Native=logical(), src=character(), stringsAsFactors=FALSE)
    
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
      
      t_f_df <- rbind(t_f_df, data.frame(ISO3=iso3, Native=native, src=src, stringsAsFactors=FALSE))
    }
    
    return(t_f_df)
  }
}

get_common_names <- function(species_name, check_exists=FALSE) {
  
  if (check_exists == TRUE) {
    if (!check_species_exists(species_name)) {
      stop("Species not in Database. Try checking master list with get_all_species()")
    }
  }
  
  usageKey <- get_usageKey(species_name)
  
  DAISIE_vernacular <- read.csv("DAISIE data/DAISIE_vernacular_names.csv", 
                                stringsAsFactors=FALSE)
  
  EPPO_names <- read.csv("EPPO data/EPPO_names.csv", 
                         stringsAsFactors=FALSE)
  
  results_dict <- list()
  
  if (usageKey %in% DAISIE_vernacular$usageKey) {
    results_dict$DAISIE_vernacular <- DAISIE_vernacular[DAISIE_vernacular$usageKey == usageKey, ]
  }
  
  results_dict$EPPO_names <- EPPO_names[EPPO_names$usageKey == usageKey, ]
  
  results_dict <- results_dict[sapply(results_dict, function(x) !is.null(x) && nrow(x) > 0)]
  
  return(results_dict)
}
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

# Function to select a specific trait table
get_trait_table <- function(table_name, usageKey = NULL) {
  if (table_name %in% get_trait_table_list()) {
    file_path <- switch(
      table_name,
      CABI_rainfall = "CABI data/CABI_tables/torainfall.csv",
      CABI_airtemp = "CABI data/CABI_tables/toairtemp.csv",
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

