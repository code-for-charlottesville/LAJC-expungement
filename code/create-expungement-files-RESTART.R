##########################################################
# This script exists to restart the processing of expungement files by
# looking through the log file and finding any dirs that were _not_ processed
# and then processing them.
#
# This was created because the original script tends to die halfway through (5+ hours in).
# At least once, it died with this message:
#
#   Error in unserialize(node$con) : 
#     Failed to retrieve the value of MultisessionFuture (<none>) from cluster RichSOCKnode #3 (PID 911825 on localhost ‘localhost’). The reason reported was ‘error reading from connection’. Post-mortem diagnostic: No process exists with this PID, i.e. the localhost worker is no longer alive.
#   Calls: future_walk ... resolved -> resolved.ClusterFuture -> receiveMessageFromWorker
#   Execution halted
#
##########################################################

library(tidyverse)
library(fs)
library(furrr)
library(here)
source(here("code", "helper-functions.R"))
source(here("code", "expunge_classifier.R"))

plan(multisession(workers = availableCores() - 1))
options('future.rng.onMisuse' = "ignore")

LOG_FILE <- here("logs", "create-expungement-files4.log")
# if (fs::file_exists(LOG_FILE)) fs::file_delete(LOG_FILE)
# fs::file_create(LOG_FILE)

# set up counts file
COUNTS_FILE <- "/home/rstudio/expunge_counts4.csv"
# if (fs::file_exists(COUNTS_FILE)) fs::file_delete(COUNTS_FILE)
# write_lines("person_id,automatic,petition,automatic_pending,petition_pending,not_eligible,old_petition,old_not_eligible", COUNTS_FILE)

person_dirs <- fs::dir_ls(PERSON_DATA_DIR)

# check if everything was run
log_lines <- read_file(LOG_FILE)
f_missing <- person_dirs[!purrr::map_lgl(person_dirs, ~stringr::str_detect(log_lines, paste0("Finished.+", .x)))]
if (length(f_missing) > 0 ) {
  cat(paste("****!!!****", Sys.time(), length(f_missing), "FILES ARE MISSING FROM THE LOGS and may not have been processed:", "\n"))
  purrr::walk(f_missing, ~ {
    cat(paste("****!!!****", "MISSING FROM LOGS:", .x, "\n"))
  })
}

write_lines(paste("***!!!***", Sys.time(), "-- RESTARTING with", length(f_missing), " directories to process..."), LOG_FILE, append = TRUE)

future_walk(f_missing, function(.d) {
  
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
      #write_expungeable_counts(res, COUNTS_FILE)
      #write_expunge_person_file(res)
      write_expunge_person_file_BIG(res)
    }
  })
  write_lines(paste("********", Sys.time(), "-- Finished", .d), LOG_FILE, append = TRUE)
})

write_lines(paste("********", Sys.time(), "-- All done with all files"), LOG_FILE, append = TRUE)

