history(Inf)
library(tidyverse)
library(rgbif) # taxonomic package
library(taxize) # taxonomic package
library(magrittr)

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


get_unmatched <- function(species_list) {
  
  tax_acc_l <- lapply(species_list, get_accepted_taxonomy)
  xtra_cols <-
    c(
      "kingdomkey",
      "phylumkey",
      "classkey",
      "orderkey",
      "specieskey",
      "note",
      "familykey",
      "genuskey",
      "scientificname",
      "canonicalname",
      "confidence"
    )
  
  suppressMessages(
    tax_acc <- tax_acc_l %>%
      purrr::reduce(full_join) %>%
      mutate(genus_species = str_squish(genus_species)) %>%
      select(-one_of(xtra_cols))
  )
  
  formatted_records <- suppressMessages(
    other_taxonomy_data <- tax_acc %>%
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
  return(all_records)
}
process_command_line_args <- function() {
  # Check if there is at least one command-line argument
  if (length(commandArgs()) <= 1) {
    stop("Usage: Rscript script.R <input_vector>")
  }
  
  # Extract the input vector from the command-line arguments
  input_vector <- as.numeric(commandArgs(trailingOnly = TRUE)[-1])
  
  # Call the get_unmatched function
  result <- get_unmatched(input_vector)
  
  # Print or return the result as needed
  return(result)
}

# Call the function to process command-line arguments and execute the script
process_command_line_args()




