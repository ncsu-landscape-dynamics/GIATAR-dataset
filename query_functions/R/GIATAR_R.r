
# File: query_functions/GIATAR_query_functions.R
# Author: Thom Worm  
# Date created: 2023-04-14
# Description: Functions to facilitate querying the GIATAR database

library(dplyr)
library(readr)

# get_species_name(usageKey) - returns species name as string - takes usageKey as string or int

get_species_name <- function(usageKey) {
  
  if(!is.character(usageKey)) {
    usageKey <- as.character(usageKey)
    usageKey <- gsub("\\.0$", "", usageKey) 
  }
  
  if(usageKey %in% invasive_all_source$usageKey) {
    return(invasive_all_source$canonicalName[invasive_all_source$usageKey == usageKey])
  }
  
}

# get_usageKey(species_name) - returns usageKey as string - takes species name as string

get_usageKey <- function(species_name) {
  
  if(species_name %in% invasive_all_source$canonicalName) {
    return(invasive_all_source$usageKey[invasive_all_source$canonicalName == species_name])  
  } else if (species_name %in% invasive_all_source$speciesASFR) {
    return(invasive_all_source$usageKey[invasive_all_source$speciesASFR == species_name])
  } else if (species_name %in% invasive_all_source$speciesEPPO) {
    return(invasive_all_source$usageKey[invasive_all_source$speciesEPPO == species_name]) 
  } else if (species_name %in% invasive_all_source$speciesCABI) {
    return(invasive_all_source$usageKey[invasive_all_source$speciesCABI == species_name])
  } else if (species_name %in% invasive_all_source$usageKey) {
    return(species_name)
  } else if (species_name %in% invasive_all_source$speciesGBIF) {
    return(invasive_all_source$usageKey[invasive_all_source$speciesGBIF == species_name])
  } else if (species_name %in% invasive_all_source$speciesDAISIE) {
    return(invasive_all_source$usageKey[invasive_all_source$speciesDAISIE == species_name])
  } else if(grepl("^\\d+$", species_name) | 
           grepl("^xx", species_name, ignore.case = TRUE) |
           grepl("^XX", species_name, ignore.case = TRUE)) {
    return(species_name) 
  } else {
    tryCatch({
      gbif <- rgbif::name_backbone(name = species_name, rank="species")
      return(as.character(gbif$usageKey))
    }, error = function(e) {
      print("species not found in Database or GBIF")
      return(NULL)
    })
  }
  
}

# get_all_species() - returns list of all species names in database - no inputs

get_all_species <- function() {
  
  species_list <- c()
  
  for(i in 1:nrow(invasive_all_source)) {
    
    row <- invasive_all_source[i,]
    
    if(row$rank %in% c("SPECIES", "FORM", "SUBSPECIES", "VARIETY")) {
      species_list <- c(species_list, row$canonicalName)  
    } else if(!is.na(row$speciesEPPO)) {
      species_list <- c(species_list, row$speciesEPPO)
    } else if(!is.na(row$speciesASFR)) {
      if(row$speciesASFR %in% species_list) {
        species_list <- c(species_list, row$speciesCABI)  
      } else {
        species_list <- c(species_list, row$speciesASFR)
      }
    } else if(!is.na(row$speciesCABI)) {
      species_list <- c(species_list, row$speciesCABI)
    }
    
  }
  
  return(unique(species_list))
  
}

# check_species_exists(species_name) - returns True or False - takes species name as string

check_species_exists <- function(species_name) {
  
  usageKey <- get_usageKey(species_name)
  
  return(usageKey %in% invasive_all_source$usageKey)
  
}

# get_first_introductions(usageKey, check_exists=False, ISO3_only=False, import_additional_native_info=True) - returns dataframe of first introductions - takes usageKey as string or int, check_exists=True will raise a KeyError if species is not in database, ISO3_only=True will return only return species location info that are 3 character ISO3 codes. Some other location info includes bioregions or other geonyms, import_additional_native_info=True will import additional native range info, first by seeing if native range info for a particular country is availible from sources that reported later than the first introduction, and second by importing native range info from the native range database

get_first_introductions <- function(species_name, check_exists=FALSE, ISO3_only=FALSE, import_additional_native_info=TRUE) {
  
  usageKey <- get_usageKey(species_name)
  
  if(check_exists) {
    if(!check_species_exists(usageKey)) {
      stop("Species not in Database. Try checking master list with get_all_species()")
    }
  }
  
  df <- first_records[first_records$usageKey == usageKey, ]
  
  if(ISO3_only) {
    df <- df[nchar(df$ISO3) == 3, ] 
  }
  
  if(import_additional_native_info) {
    
    native_ranges <- get_native_ranges(usageKey, ISO3 = unique(df$ISO3))
    native_ranges <- native_ranges[!is.na(native_ranges$Native),]
    
    for(i in 1:nrow(native_ranges)) {
      if(native_ranges[i, "Native"] == TRUE) {
        df[df$ISO3 == native_ranges[i, "ISO3"], "Native"] <- TRUE  
      } else {
        df[df$ISO3 == native_ranges[i, "ISO3"], "Native"] <- FALSE
      }
    }
    
    return(df[!df$ISO3 %in% c("ZZ", "XL", "XZ"),])
    
  } else {
    return(df[!df$ISO3 %in% c("ZZ", "XL", "XZ"),])
  }
  
}

# get_all_introductions(usageKey, check_exists=False, ISO3_only=True) - returns dataframe of all introductions - takes usageKey as string or int, check_exists=True will raise a KeyError if species is not in database, ISO3_only=True will return only return species location info that are 3 character ISO3 codes. Some other location info includes bioregions or other geonyms

get_all_introductions <- function(species_name, check_exists=FALSE, ISO3_only=TRUE, import_additional_native_info=TRUE) {

  usageKey <- get_usageKey(species_name)
  
  if(check_exists) {
    if(!check_species_exists(usageKey)) {
      stop("Species not in Database. Try checking master list with get_all_species()")
    }
  }
  
  if(is.numeric(usageKey)) {
    usageKey <- as.character(usageKey)
  }
  
  if(!grepl("^\\d+$", usageKey)) {
    usageKey <- get_usageKey(usageKey) 
  }
  
  # Read the all_records.csv file here
  # e.g., all_records <- read_csv("path/to/all_records.csv", col_types = cols(usageKey = col_character()))
  
  df <- all_records[all_records$usageKey == usageKey, ]
  
  if(ISO3_only) {
    df <- df[nchar(df$ISO3) == 3, ]
  }
  
  if(import_additional_native_info) {
    
    native_ranges <- get_native_ranges(usageKey, ISO3 = unique(df$ISO3))
    native_ranges <- native_ranges[!is.na(native_ranges$Native),]
    
    for(i in 1:nrow(native_ranges)) {
      if(native_ranges[i, "Native"] == TRUE) {
        df[df$ISO3 == native_ranges[i, "ISO3"], "Native"] <- TRUE  
      } else {
        df[df$ISO3 == native_ranges[i, "ISO3"], "Native"] <- FALSE
      }
    }
    
    return(df[!df$ISO3 %in% c("ZZ", "XL", "XZ"),])
    
  } else {
    return(df[!df$ISO3 %in% c("ZZ", "XL", "XZ"),])
  }
  
}

# get_ecology(species_name) - returns dictionary of dataframes of ecology info - takes species name as string

get_ecology <- function(species_name, check_exists=FALSE) {
  
  if(check_exists) {
    if(!check_species_exists(species_name)) {
      stop("Species not in Database. Try checking master list with get_all_species()") 
    }
  }
  
  usageKey <- get_usageKey(species_name)
  
  result_list <- list()
  
  # Read the CSV files here
  # e.g., CABI_rainfall <- read_csv("path/to/CABI_rainfall.csv", col_types = cols(usageKey = col_character()))
  
  result_list[["CABI_rainfall"]] <- CABI_rainfall[CABI_rainfall$usageKey == usageKey, ]
  result_list[["CABI_airtemp"]] <- CABI_airtemp[CABI_airtemp$usageKey == usageKey, ]
  result_list[["CABI_climate"]] <- CABI_climate[CABI_climate$usageKey == usageKey, ]
  result_list[["CABI_environments"]] <- CABI_environments[CABI_environments$usageKey == usageKey, ]
  result_list[["CABI_lat_alt"]] <- CABI_lat_alt[CABI_lat_alt$usageKey == usageKey, ]
  result_list[["CABI_water_tolerances"]] <- CABI_water_tolerances[CABI_water_tolerances$usageKey == usageKey, ]
  result_list[["CABI_wood_packaging"]] <- CABI_wood_packaging[CABI_wood_packaging$usageKey == usageKey, ]
  result_list[["DAISIE_habitats"]] <- DAISIE_habitats[DAISIE_habitats$usageKey == usageKey, ]
  
  result_list <- result_list[lapply(result_list, nrow) > 0]
  
  return(result_list)
  
}

# get_hosts_and_vectors(species_name) - returns dictionary of dataframes of host and vector info - takes species name as string

get_hosts_and_vectors <- function(species_name, check_exists=FALSE) {
  
  if(check_exists) {
    if(!check_species_exists(species_name)) {
      stop("Species not in Database. Try checking master list with get_all_species()") 
    }
  }
  
  usageKey <- get_usageKey(species_name)
  
  # Read the CSV files here
  # e.g., CABI_tohostPlants <- read_csv("path/to/CABI_tohostPlants.csv", col_types = cols(usageKey = col_character()))
  
  results_list <- list()
  
  results_list[["CABI_tohostPlants"]] <- CABI_tohostPlants[CABI_tohostPlants$usageKey == usageKey, ]
  results_list[["CABI_topathwayVectors"]] <- CABI_topathwayVectors[CABI_topathwayVectors$usageKey == usageKey, ]
  results_list[["CABI_tovectorsAndIntermediateHosts"]] <- CABI_tovectorsAndIntermediateHosts[CABI_tovectorsAndIntermediateHosts$usageKey == usageKey, ]
  results_list[["EPPO_hosts"]] <- EPPO_hosts[EPPO_hosts$usageKey == usageKey, ]
  results_list[["DAISIE_pathways"]] <- DAISIE_pathways[DAISIE_pathways$usageKey == usageKey, ]
  results_list[["DAISIE_vectors"]] <- DAISIE_vectors[DAISIE_vectors$usageKey == usageKey, ]
  
  results_list <- results_list[lapply(results_list, nrow) > 0]
  
  return(results_list)
  
}

# get_species_list(kingdom=NULL, phylum=NULL, taxonomic_class=NULL, order=NULL, family=NULL, genus=NULL) - returns list of usageKeys matching taxonomic criteria - takes kingdom, phylum, taxonomic_class, order, family, genus as strings  

get_species_list <- function(kingdom=NULL, phylum=NULL, taxonomic_class=NULL, order=NULL, family=NULL, genus=NULL) {
  
  # Read the GBIF_backbone_invasive.csv file here
  # e.g., GBIF_backbone_invasive <- read_csv("path/to/GBIF_backbone_invasive.csv")
  
  if(!is.null(kingdom)) {
    GBIF_backbone_invasive <- GBIF_backbone_invasive[GBIF_backbone_invasive$kingdom == kingdom, ]
  }
  
  if(!is.null(phylum)) {
    GBIF_backbone_invasive <- GBIF_backbone_invasive[GBIF_backbone_invasive$phylum == phylum, ]
  }
  
  if(!is.null(taxonomic_class)) {
    GBIF_backbone_invasive <- GBIF_backbone_invasive[GBIF_backbone_invasive$class == taxonomic_class, ]
  }
  
  if(!is.null(order)) {
    GBIF_backbone_invasive <- GBIF_backbone_invasive[GBIF_backbone_invasive$order == order, ] 
  }
  
  if(!is.null(family)) {
    GBIF_backbone_invasive <- GBIF_backbone_invasive[GBIF_backbone_invasive$family == family, ]
  }
  
  if(!is.null(genus)) {
    GBIF_backbone_invasive <- GBIF_backbone_invasive[GBIF_backbone_invasive$genus == genus, ]
  }
  
  usageKey_list <- unique(GBIF_backbone_invasive$usageKey)
  
  return(usageKey_list)
  
}

get_native_ranges <- function(species_name, ISO3=NULL, check_exists=FALSE) {
  
  if(check_exists) {
    if(!check_species_exists(species_name)) {
      stop("Species not in Database. Try checking master list with get_all_species()")
    }
  }
  
  usageKey <- get_usageKey(species_name)
  
  if(is.null(ISO3)) {
    
    # Read the all_records.csv file here
    # e.g., all_records <- read_csv("path/to/all_records.csv", col_types = cols(usageKey = col_character()))
    
    records <- all_records[all_records$usageKey == usageKey, ]
    records <- records[!is.na(records$Native), ]
    records <- records[records$Source != "Original", ]
    
    records <- records[, c("ISO3", "Source", "Native", "Reference")]
    records$bioregion <- NA
    records$DAISIE_region <- NA
    
    # Read the native_ranges file here
    # e.g., native_ranges <- read_csv("path/to/native_ranges.csv", col_types = cols(usageKey = col_character()))
    
    native_ranges_temp <- native_ranges[native_ranges$usageKey == usageKey, c("Source", "bioregion", "DAISIE_region", "Reference")]
    native_ranges_temp$Native <- TRUE
    native_ranges_temp$ISO3 <- NA
    
    records <- rbind(records, native_ranges_temp)
    
    return(records)
    
  } else {
    
    tryCatch({
      
      if(!is.list(ISO3)) {
        stop("ISO3 must be a list of 3 character strings")
      }
      
      native_ranges$usageKey <- as.character(native_ranges$usageKey)
      
      records <- all_records[all_records$usageKey == usageKey, ]
      records <- records[!is.na(records$Native), ]
      records <- records[records$Source != "Original", ]
      
      records <- records[, c("ISO3", "Source", "Native", "Reference")]
      records$bioregion <- NA
      records$DAISIE_region <- NA
      
      t_f_df <- data.frame(ISO3 = character(), Native = logical(), src = character())
      
      bioregions <- unique(native_ranges$bioregion[native_ranges$usageKey == usageKey])
      
      for(iso3 in ISO3) {
        native <- NA
        src <- NA
        
        if(length(bioregions) > 0) {
          bioregions_temp <- native_range_crosswalk$modified_Bioregion[native_range_crosswalk$ISO3 == iso3]
          for(br in bioregions_temp) {
            if(br %in% bioregions) {
              native <- TRUE
              src <- "br"
              break
            } else {
              native <- FALSE 
              src <- "br"
            }
          }
        }
        
        if(iso3 %in% records$ISO3) {
          native <- records$Native[records$ISO3 == iso3]
          src <- "records"
        }
        
        if(length(bioregions) == 0 & !iso3 %in% records$ISO3) {
          native <- NA
          src <- "records only - no bioregion found" 
        }
        
        t_f_df <- rbind(t_f_df, data.frame(ISO3 = iso3, Native = native, src = src))
        
      }
      
      return(t_f_df)
      
    }, error = function(e) {
      print("ISO3 missing from bioregion crosswalk")
      print(paste0("please add ", iso3, " to bioregion crosswalk"))
      return(NULL) 
    })
    
  }
  
}

get_common_names <- function(species_name, check_exists=FALSE) {
  
  if(check_exists) {
    if(!check_species_exists(species_name)) {
      stop("Species not in Database. Try checking master list with get_all_species()") 
    }
  }
  
  usageKey <- get_usageKey(species_name)
  
  # Read the DAISIE_vernacular and EPPO_names files here
  # e.g., DAISIE_vernacular <- read_csv("path/to/DAISIE_vernacular.csv", col_types = cols(usageKey = col_character()))
  # e.g., EPPO_names <- read_csv("path/to/EPPO_names.csv", col_types = cols(usageKey = col_character()))
  
  results_list <- list()
  
  results_list[["DAISIE_vernacular"]] <- DAISIE_vernacular[DAISIE_vernacular$usageKey == usageKey, ]
  results_list[["EPPO_names"]] <- EPPO_names[EPPO_names$usageKey == usageKey, ]
  
  results_list <- results_list[lapply(results_list, nrow) > 0]
  
  return(results_list)
  
}
    
