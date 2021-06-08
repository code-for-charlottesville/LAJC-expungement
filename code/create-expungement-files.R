library(tidyverse)
library(fs)
library(here)
source(here("code", "helper-functions.R"))
source(here("code", "expunge_classifier.R"))

LOG_FILE <- here("logs", "create-expungement-files.log")
if (fs::file_exists(LOG_FILE)) fs::file_delete(LOG_FILE)
fs::file_create(LOG_FILE)

# set up counts file
COUNTS_FILE <- "/home/rstudio/expunge_counts.csv"
if (fs::file_exists(COUNTS_FILE)) fs::file_delete(COUNTS_FILE)
write_lines("person_id,automatic,petition,not_eligible,old_petition,old_not_eligible", COUNTS_FILE)

person_dirs <- fs::dir_ls("/home/rstudio/persondata")
write_lines(paste("***", Sys.time(), "-- Found", length(person_dirs), " directories to process..."), LOG_FILE, append = TRUE)

walk(person_dirs, function(.d) {
  
  person_files <- fs::dir_ls(.d) %>% 
    str_subset(".lck$", negate = TRUE) %>% 
    basename()
  
  write_lines(paste("***", Sys.time(), "-- Found", length(person_files), "people in", .d, ", starting to process..."), LOG_FILE, append = TRUE)
  
  walk(person_files, function(.f) {
    res <- tryCatch({
      suppressWarnings(classify_ex(.f))
    }, 
    error = function(.e) {
      write_lines(paste("********", Sys.time(), "--", .f, "-- ERROR:", .e$message), LOG_FILE, append = TRUE)  
      return(data.frame())
    })
    
    # write out results, if any
    if (nrow(res) > 0) {
      write_expungeable_counts(res, COUNTS_FILE)
      write_expunge_person_file(res)
    }
  })
  write_lines(paste("********", Sys.time(), "-- Finished", .d), LOG_FILE, append = TRUE)
})

write_lines(paste("********", Sys.time(), "-- All done with all files"), LOG_FILE, append = TRUE)
