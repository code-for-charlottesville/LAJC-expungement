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
write_lines(paste("***", Sys.time(), "-- Found", length(criminal_files), "to process..."), LOG_FILE, append = TRUE)

# Use this for testing, and comment out the future_walk() below until you're ready to rock
# .d <- read_court_file(criminal_files[100])
# pwalk(slice_head(.d, n=1000), write_to_person_file)

future_walk(criminal_files, function(.f) {
  write_lines(paste("********", Sys.time(), "-- Starting", .f), LOG_FILE, append = TRUE)
  tryCatch({
    .d <- read_court_file(.f)
    pwalk(.d, write_to_person_file)    
  }, 
  error = function(.e) {
    write_lines(paste("********", Sys.time(), "-- ERROR:", .e$message), LOG_FILE, append = TRUE)  
  })
  write_lines(paste("********", Sys.time(), "-- Finished", .f), LOG_FILE, append = TRUE)
})

write_lines(paste("********", Sys.time(), "-- All done with all files"), LOG_FILE, append = TRUE)
