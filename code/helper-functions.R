suppressPackageStartupMessages(library(tidyverse))
suppressPackageStartupMessages(library(data.table))
suppressPackageStartupMessages(library(filelock))

PERSON_DATA_DIR <- "~/persondata"

#####################
# reading court data
#####################


read_district_file <- function(.f) {
  fread(
    .f,
    select = c(
      "HearingDate",
      "person_id",
      "CodeSection",
      "FinalDisposition",
      "CaseType",
      "Charge"
    )
  ) %>% 
    rename(
      DispositionCode = FinalDisposition,
      ChargeType = CaseType
    ) %>%
    mutate(
      CourtType = "district"
    )
}

read_circuit_file <- function(.f) {
  fread(
    .f,
    select = c(
      "HearingDate",
      "person_id",
      "CodeSection",
      "DispositionCode",
      "ChargeType",
      "Charge"  
    )
  ) %>%
    mutate(
      CourtType = "circuit"
    )
}

read_court_file <- function(.f) {
  if (str_detect(.f, "district")) {
    read_district_file(.f)
  } else if (str_detect(.f, "circuit")) {
    read_circuit_file(.f)
  } else {
    stop(paste("unknown file type:", .f))
  }
}


###############################
# extracting person-level data
###############################

# use this with pwalk
write_to_person_file <- function(...) {
  .row <- list(...)
  
  person_file <- file.path(PERSON_DATA_DIR, .row$person_id)
  if (!fs::file_exists(person_file)) fs::file_create(person_file)
  
  # lock file so no other process can write to it at the same time
  .l <- lock(paste0(person_file, ".lck"), timeout = 5000)
  on.exit(unlock(.l))
  if (is.null(.l)) {
    warning(paste("Could not access", person_file, "because of lockfile problems."))
  }
  
  person_string <- paste(
    .row$person_id,
    .row$HearingDate,
    .row$CodeSection,
    .row$ChargeType,
    .row$Class,
    .row$DispositionCode,
    sep = ","
  )
  
  write_lines(person_string, person_file, append = TRUE)
}


read_person_file <- function(.pid) {
  read_csv(
    file.path(PERSON_DATA_DIR, .pid),
    col_names = c(
      "person_id",
      "HearingDate",
      "CodeSection",
      "ChargeType",
      "Class",
      "DispositionCode"
    ),
    col_types = "cDcccc"
  )
}
