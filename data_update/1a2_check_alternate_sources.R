###
# File: data_update/1a2_check_alternate_sources.R
# Author: Thom Worm
# Date created: 2023-04-14
# Description: Check alternate sources for species names that were not matched to GBIF
###

history(Inf)
library(tidyverse)
library(rgbif) # taxonomic package
library(taxize) # taxonomic package
library(magrittr)
library(dotenv)

dotenv::load_dot_env()
data_path <- Sys.getenv("DATA_PATH")

data_path = paste0(data_path,"/species lists/gbif_matched")
setwd(data_path)

cabi_gbif <- read_csv("cabi_gbif.csv", col_types = cols(.default = "c"))
eppo_gbif <- read_csv("eppo_gbif.csv", col_types = cols(.default = "c"))
asfr_gbif <- read_csv("asfr_gbif.csv", col_types = cols(.default = "c"))

# Combine the data frames into a list for easy iteration
datasets <- list(cabi_gbif, eppo_gbif, asfr_gbif)

species_with_na_usageKey <- c()
# Loop through each dataset
for (df in datasets) {
  # Extract species names with NA usageKeys and add to the vector
  species_with_na_usageKey <- c(species_with_na_usageKey, df$species[is.na(df$usageKey)])
}

# Print the unique species names with NA usageKeys
unique_species_with_na <- unique(species_with_na_usageKey)
unique_species_with_na

eppo_gbif[is.na(eppo_gbif$usageKey),]
get_more_info <- function(taxa_name) {
  print(taxa_name)
  id = c("")
  try(id <-
        gnr_resolve(
          sci = taxa_name,
          data_source_ids = c(1, 2, 3, 4, 8, 12, 152, 169, 168),
          canonical = TRUE,
          best_match_only = TRUE
        ))
  # deal with cases where species name not found
  id_res <-
    if (id == "" ||
        nrow(id) == 0) {
      data.frame(user_supplied_name = taxa_name,
                 matched_name2 = "species not found")
    } else {
      tax_ids <- id %>%
        dplyr::rename(taxonomy_system = data_source_title) %>%
        select(-submitted_name,-score)
    }
  
  if (id_res$matched_name2 == "species not found") {
    return(id_res)
    
  } else {
    # get higher taxonomic classification
    ranks <-
      c("kingdom",
        "phylum",
        "class",
        "order",
        "family",
        "genus",
        "species")
    
    id_class <-
      if (!(id_res$taxonomy_system %in% c("NCBI", "ITIS"))) {
        id_res
      } else {
        Sys.sleep(4)
        c <- tax_name(
          taxa_name,
          get = c(
            "kingdom",
            "phylum",
            "class",
            "order",
            "family",
            "genus",
            "species"
          ),
          db = tolower(id_res$taxonomy_system)
        )
        if ((length(c$species) == 0) ||
            sum(length(c$kingdom) + length(c$genus) + length(c$order)) == 0 ||
            (is.na(c$kingdom) | is.na(c$genus) | is.na(c$order))) {
          id_res
        } else {
          Sys.sleep(6)
          uid <- get_uid(c$species, rows = 1)
          if (is.null(uid[[1]])) {
            id_res
          } else {
            c3 <- bind_cols(c, data.frame(uid[[1]])) %>%
              rename(uid = uid..1.., user_supplied_name = query) %>%
              select(-db)
          }
        }
      }
    
    # merge id_res and id_class
    id_all <-
      if (all(names(id_res) %in% names(id_class)) == TRUE) {
        id_res
      } else {
        id_res %>%
          full_join(id_class, by = c("user_supplied_name")) %>%  # bind in the taxonomic names
          mutate_if(is.factor, as.character)
      }
    
    return(id_all)
  }
}

get_accepted_taxonomy <- function(taxa_name) {
  # get taxa ids, authoritative names, and names higher up
  id <- get_gbifid_(taxa_name)  # gets ID from GBIF
  
  # deal with cases where species name not found
  if (nrow(id[[1]]) == 0) {
    data.frame(user_supplied_name = taxa_name,
               genus_species = "species not found")
    
  } else {
    xtra_cols <-
      c(
        #"rank", "status", "matchtype", "confidence", "synonym", "acceptedusagekey",
        "kingdomkey",
        "phylumkey",
        "classkey",
        "orderkey",
        "specieskey",
        "note",
        "familykey",
        "genuskey"
      )
    
    # puts ID info into one dataframe
    tax_id <-
      map_df(id, ~ as.data.frame(.x), .id = "user_supplied_name")
    
    
    id_insect <- tax_id %>%
      mutate_if(is.logical, as.character)
    #   # filter to kingdom, phylum, class
    #   dplyr::filter(kingdom == "Animalia"  | is.na(kingdom)) %>%
    #   dplyr::filter(if(!("phylum" %in% names(.))) {TRUE} else {
    #     phylum == "Arthropoda" | is.na(phylum)}) %>%
    #   dplyr::filter(if(!("class" %in% names(.))) {TRUE} else {
    #     class == "Insecta"  | is.na(class)})
    #
    if (nrow(id_insect) == 0) {
      id_insect
    } else {
      # filter dataframe for accepted names
      id_acc <- id_insect %>%
        # filter to best matched name
        dplyr::filter(if (sum(status %in% c("ACCEPTED") &
                              matchtype %in% c("EXACT")) > 0) {
          status == "ACCEPTED" & matchtype == "EXACT"
        } else if (sum(
          status %in% c("SYNONYM") &
          matchtype %in% c("EXACT") & rank != "genus"
        ) > 0) {
          status == "SYNONYM" & matchtype == "EXACT" & rank != "genus"
        } else if (sum(status %in% c("DOUBTFUL") &
                       matchtype %in% c("EXACT")) > 0) {
          status == "DOUBTFUL" & matchtype == "EXACT"
        } else if (sum(status %in% c("ACCEPTED") &
                       matchtype %in% c("HIGHERRANK")) > 0) {
          status == "ACCEPTED" & matchtype == "HIGHERRANK"
        } else if (sum(status %in% c("DOUBTFUL") &
                       matchtype %in% c("HIGHERRANK")) > 0) {
          status == "DOUBTFUL" & matchtype == "HIGHERRANK"
        } else if (sum(
          status %in% c("SYNONYM") &
          matchtype %in% c("EXACT") & rank == "genus"
        ) > 0) {
          status == "SYNONYM" & matchtype == "EXACT" & rank == "genus"
        } else if (sum(status %in% c("ACCEPTED") &
                       matchtype %in% c("FUZZY")) > 0) {
          status == "ACCEPTED" & matchtype == "FUZZY"
        } else if (sum(status %in% c("SYNONYM") &
                       matchtype %in% c("FUZZY")) > 0) {
          status == "SYNONYM" & matchtype == "FUZZY"
        } else {
          row_number() == 1
        }) %>%
        dplyr::filter(xor(any(
          rank %in% c("species", "subspecies", "form", 'variety')
        ),
        any(
          rank %in% c("genus", "family", "order", "class", "phylum", "kingdom")
        ))) %>% # filter rank to species if both genus and species, and filter to genus if both genus and family # Rebecca changed, also the mutate lines below
        dplyr::select(-one_of(xtra_cols))
      id_acc <-
        if (nrow(id_acc) > 1) {
          id_acc[1, ]
        } else {
          id_acc
        } # if more than one row, select first row
      # make df of all taxonomic info from GBIF
      
      tax_gbif <- id_acc %>%
        # get authority
        mutate(taxonomic_authority = ifelse(
          sapply(strsplit(scientificname, " "), length) == 1,
          NA_character_,
          gsub("^\\w+\\s+\\w+\\s+(.*)", "\\1", scientificname)
        )) %>%
        mutate(
          taxonomic_authority = ifelse((exists("genus") &&
                                          (
                                            genus %in% sapply(strsplit(taxonomic_authority, " "), unlist)
                                          )) |
                                         user_supplied_name %in% sapply(strsplit(taxonomic_authority, " "), unlist),
                                       stringr::word(taxonomic_authority, -2, -1),
                                       taxonomic_authority
          )
        ) %>%
        # get genus_species
        mutate(genus_species = ifelse((!exists("species") |
                                         is.na("species")) & exists("genus"),
                                      paste(genus, "sp"),
                                      ifelse(exists("species"), species, "undetermined")
        ))  %>%
        mutate(taxonomy_system = "GBIF") %>% # fill in taxonomy system source
        select(-scientificname,-canonicalname,-confidence) %>%
        mutate_if(is.logical, as.character)
      
      return(tax_gbif)
    }
  }
}

###################################################
###################################################
#Matching Names
###################################################



tax_go_l <- lapply(unique_species_with_na, get_more_info)
# make dataframe of all results
suppressMessages( #this coerces results into a table
  other_taxonomy_data <- tax_go_l %>%
    purrr::reduce(full_join) %>% # join all data frames from list
    dplyr::filter(!(matched_name2 == "species not found")) %>% # remove taxa that didn't provide a species-level match (no new info)
    dplyr::filter((str_count(
      matched_name2, '\\s+'
    ) + 1) %in% c(2, 3)) %>%
    mutate(
      genus = ifelse((str_count(matched_name2, '\\s+') + 1) == 1,
                     matched_name2,
                     stringr::word(matched_name2, 1)
      ),
      species = ifelse((str_count(
        matched_name2, '\\s+'
      ) + 1) %in% c(2, 3), matched_name2, NA_character_),
      genus_species = ifelse(is.na(species), paste(genus, "sp"), species)
    ) %>%
    select(-matched_name2)
)

#other taxonomy data is a df that contains the unmatched names that were coerced from other, non-gbif data soruces
check_gbif <- other_taxonomy_data$genus_species
check_gbif_apply <- lapply( check_gbif, get_accepted_taxonomy)

suppressMessages(
  gbif_results <- check_gbif_apply %>%
    purrr::reduce(full_join) %>%
    mutate(genus_species = str_squish(genus_species)) %>%
    select(-one_of(xtra_cols))
)

filtered_gbif_results <- gbif_results[!is.na(gbif_results$acceptedusagekey), ]

# Create a new column 'usageKey' in other_taxonomy_data with NA values
other_taxonomy_data$usageKey <- NA

# Loop through each row in gbif_results to find matches in other_taxonomy_data and update usageKey
for (i in 1:nrow(gbif_results)) {
  match_index <- which(other_taxonomy_data$genus_species == gbif_results$user_supplied_name[i])
  
  if (length(match_index) > 0) {
    other_taxonomy_data$usageKey[match_index] <- gbif_results$acceptedusagekey[i]
  }
}
other_taxonomy_data


########################################################################
########################################################################
# now update with any found usagekeys - otherwise generate unique keys

generate_usageKey <- function(species) {
  words <- unlist(strsplit(species, " ")) # Split species column into words
  if (length(words) >= 2) {
    usageKey <- paste0("XX", substr(words[1], 1, 3), substr(words[2], 1, 3))
    if (length(words) >= 3) {
      usageKey <- paste0(usageKey, substr(words[3], 1, 1))
    }
  } else {
    usageKey <- paste0("XX", substr(words[1], 1, 3))
  }
  return(usageKey)
}
generated_usageKey_species <- data.frame(species = character(0), usageKey = character(0)) # Empty dataframe to store species names and generated usage keys


update_usageKey <- function(df, other_taxonomy_data) {
  for (i in seq_len(nrow(df))) {
    # Check if usageKey is NA for the current row
    if (is.na(df$usageKey[i])) {
      species_match <- df$species[i] %in% other_taxonomy_data$user_supplied_name
      
      # Check if the species is in other_taxonomy_data and has a non-NA usageKey
      if (species_match && !is.na(other_taxonomy_data$usageKey[match(df$species[i], other_taxonomy_data$user_supplied_name)])) {
        df$usageKey[i] <- other_taxonomy_data$usageKey[match(df$species[i], other_taxonomy_data$user_supplied_name)]
      } else {
        # Generate usageKey if species is not found or has NA in other_taxonomy_data
        generated_key <- generate_usageKey(df$species[i])
        generated_usageKey_species <- rbind(generated_usageKey_species, data.frame(species = df$species[i], usageKey = generated_key))
        df$usageKey[i] <- generated_key
      }
    }
  }
  return(df)
}
other_taxonomy_data
# Loop through each dataframe and update usageKey column based on conditions
source_dataframes <- list(cabi_gbif, asfr_gbif, eppo_gbif)
for (i in seq_along(source_dataframes)) {
  df <- update_usageKey(source_dataframes[[i]], other_taxonomy_data)
  
  # Check if species column in the source dataframe matches and has a non-NA usageKey in other_taxonomy_data
  
  # Update usageKey column based on matches and generate_usageKey function for non-NA usageKey values
  
  
  source_dataframes[[i]] <- df
}
source_dataframes


# Display updated source dataframes with usageKey column
cabi_gbif <- source_dataframes[[1]]
asfr_gbif <- source_dataframes[[2]]
eppo_gbif <- source_dataframes[[3]]

print(cabi_gbif)
print(asfr_gbif)
print(eppo_gbif)
write.csv(generated_usageKey_species, file = "generated_usageKey_species.csv", row.names = FALSE)

write.csv(cabi_gbif, file = "cabi_gbif.csv", row.names = FALSE)
write.csv(asfr_gbif, file = "asfr_gbif.csv", row.names = FALSE)
write.csv(eppo_gbif, file = "eppo_gbif.csv", row.names = FALSE)

print_rows_with_XX <- function(df) {
  rows_with_XX <- df[grep("XX", df$usageKey), ]
  print(rows_with_XX)
}

# Print rows containing "XX" in usageKey column for each dataframe
cat("Rows containing 'XX' in cabi_gbif:\n")
print_rows_with_XX(cabi_gbif)

cat("\nRows containing 'XX' in asfr_gbif:\n")
print_rows_with_XX(asfr_gbif)

cat("\nRows containing 'XX' in eppo_gbif:\n")
print_rows_with_XX(eppo_gbif)

