suppressPackageStartupMessages(library(tidyverse))
suppressPackageStartupMessages(library(data.table))

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

