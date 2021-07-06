suppressPackageStartupMessages(library(tidyverse))
suppressPackageStartupMessages(library(data.table))
suppressPackageStartupMessages(library(filelock))

PERSON_DATA_DIR <- "~/persondata"
EXPUNGE_DATA_DIR <- "~/expungedata"

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
  
  # build path
  .row$person_id <- as.character(.row$person_id)
  .dir <- file.path(PERSON_DATA_DIR, substr(.row$person_id, 1, 5))
  person_file <- file.path(.dir, .row$person_id)
  
  if (!fs::dir_exists(.dir)) fs::dir_create(.dir)
  if (!fs::file_exists(person_file)) fs::file_create(person_file)
  
  # lock file so no other process can write to it at the same time
  .l <- lock(paste0(person_file, ".lck"), timeout = 5000)
  on.exit(unlock(.l))
  if (is.null(.l)) {
    warning(paste("Could not access", person_file, "because of lockfile problems."))
  }
  
  # paste together into a csv row 
  # (removing any commas in the fields that would mess up parsing)
  person_string <- paste(
    str_replace(.row$person_id, ",", ".."),
    str_replace(.row$HearingDate, ",", ".."),
    str_replace(.row$CodeSection, ",", ".."),
    str_replace(.row$ChargeType, ",", ".."),
    str_replace(.row$Class, ",", ".."),
    str_replace(.row$DispositionCode, ",", ".."),
    str_replace(.row$Plea, ",", ".."),
    sep = ","
  )
  
  write_lines(person_string, person_file, append = TRUE)
}


read_person_file <- function(.pid) {
  .pid <- as.character(.pid)
  .dir <- file.path(PERSON_DATA_DIR, substr(.pid, 1, 5))
  person_file <- file.path(.dir, .pid)
  
  read_csv(
    file.path(person_file),
    col_names = c(
      "person_id",
      "HearingDate",
      "CodeSection",
      "ChargeType",
      "Class",
      "DispositionCode",
      "Plea"
    ),
    col_types = "cDccccc"
  )
}


write_expungeable_counts <- function(res, outfile) {
  
  person_id <- res$person_id[1]
  automatic <- sum(res$expungable == "Automatic")
  petition <- sum(res$expungable == "Petition")
  not_eligible <- sum(res$expungable == "Not eligible")
  old_petition <- sum(res$old_expunge)
  old_not_eligible <- sum(res$old_expunge == FALSE)
  automatic_pending <- sum(res$expungable_pending == "Automatic")
  petition_pending <- sum(res$expungable_pending == "Petition")
  not_eligible_pending <- sum(res$expungable_pending == "Not eligible")
  
  out_string <- paste(
    person_id,
    automatic,
    petition,
    not_eligible,
    old_petition,
    old_not_eligible,
    sep = ","
  )
  
  # lock file so no other process can write to it at the same time
  .l <- lock(outfile, timeout = 5000)
  on.exit(unlock(.l))
  if (is.null(.l)) {
    warning(paste("Could not access", person_file, "because of lockfile problems."))
  }
  
  write_lines(out_string, outfile, append = TRUE)
}


write_expunge_person_file <- function(res) {
  # build path
  person_id <- as.character(res$person_id[1])
  .dir <- file.path(EXPUNGE_DATA_DIR, substr(person_id, 1, 5))
  person_file <- file.path(.dir, person_id)
  
  if (!fs::dir_exists(.dir)) fs::dir_create(.dir)
  write_csv(res, person_file)
}