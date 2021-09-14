library(tidyverse)
library(fs)
library(furrr)
library(here)
source(here("code", "helper-functions.R"))
source(here("code", "expunge_classifier.R"))

LOG_FILE <- here("logs", "create-expungement-files3.log")
person_dirs <- fs::dir_ls(PERSON_DATA_DIR)

# check if everything was run
log_lines <- read_file(LOG_FILE)
f_missing <- person_dirs[!purrr::map_lgl(person_dirs, ~stringr::str_detect(log_lines, .x))]
if (length(f_missing) > 0 ) {
  cat(paste("****!!!****", Sys.time(), length(f_missing), "FILES ARE MISSING FROM THE LOGS and may not have been processed:", "\n"))
  purrr::walk(f_missing, ~ {
    cat(paste("****!!!****", "MISSING FROM LOGS:", .x, "\n"))
  })
}
