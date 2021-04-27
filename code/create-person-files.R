library(furrr)
library(tidyverse)
library(fs)
library(here)
source(here("code", "helper-functions.R"))

LOG_FILE <- "create-person-files.log"
if (fs::file_exists(LOG_FILE)) fs::file_delete(LOG_FILE)
fs::file_create(LOG_FILE)

plan(multisession(workers = availableCores() - 1))

criminal_files <- fs::dir_ls("/home/rstudio/courtdata") %>% str_subset("criminal")

future_walk(criminal_files, function(.f) {
  write_lines(paste("********", Sys.time(), "-- Starting", .f), LOG_FILE, append = TRUE)
  .d <- read_court_file(.f)
  pwalk(.d, write_to_person_file)
  write_lines(paste("********", Sys.time(), "-- Finished", .f), LOG_FILE, append = TRUE)
})
