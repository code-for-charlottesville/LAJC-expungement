# first you need to install these packages
library(tidyverse)
library(lubridate)
library(fs)
library(here)
library(glue)
source(here("code", "helper-functions.R"))


###################################
# function that takes a single row
###################################

# use this with pmap_dfr()
build_features <- function(...) {
  .row <- list(...)
  this_date <- as.Date(.row$HearingDate)
  
  .pdata <- read_person_file(.row$person_id) %>%
    mutate(diff = time_length(difftime(this_date, HearingDate), "years")) %>%
    filter(
      !(HearingDate == .row$HearingDate & CodeSection == .row$CodeSection), # filter out this case
      diff > 0 # filter out cases in the future
    )
  
  #### should we filter out all cases from the same day or not?
  #### if so, change `diff > 0` to `diff >= 0`
  #### if we _don't_ then the seven year, etc. rules will catch those same day cases
  #### maybe build same_day first and then throw these out?
  
  # seven year rule
  seven_year_df <- .pdata %>%
    filter(DispositionCode == "Guilty" | DispositionCode == "Guilty In Absentia") %>% # convictions
    filter(diff <= 7) # in the past 7 years 
  .row$seven_year <- nrow(seven_year_df) > 0
  
  # felony convictions
  felony_df <- .pdata %>%
    filter(DispositionCode == "Guilty" | DispositionCode == "Guilty In Absentia") %>% # convictions
    filter(ChargeType == "Felony") # for felonies
  
  .row$ten_year <- nrow(filter(felony_df, diff <= 10)) > 0
  
  .row$class_12 <- nrow(filter(felony_df, Class == 1 | Class == 2)) > 0
  
  .row$class_34 <- nrow(filter(felony_df, diff <= 20 & (Class == 3 | Class == 4))) > 0
  
  as_tibble(.row)
  
}


###################################
# test on a single file
###################################


TEST_FILE <- "/home/rstudio/courtdata/district_criminal_2016_anon_00.csv"
ROWS_TO_TEST <- 500

in_data <- read_court_file(TEST_FILE)

system.time(
  out_data <- pmap_dfr(slice_head(in_data, n = ROWS_TO_TEST), build_features)
)
# user  system elapsed 
# 11.547   0.174  10.439 
## this took 10 seconds with only 500 rows. We might need to figure out how to speed that up...

#View(out_data)

cat(glue("{ROWS_TO_TEST}\ttotal rows"))
# 500	total rows
cat(glue("{nrow(filter(out_data, seven_year))}\trows with seven-year rule"))
# 133	rows with seven-year rule
cat(glue("{nrow(filter(out_data, ten_year))}\trows with ten-year rule"))
# 13	rows with ten-year rule
cat(glue("{nrow(filter(out_data, class_12))}\trows with Class 1 & 2 rule"))
# 0	  rows with Class 1 & 2 rule
cat(glue("{nrow(filter(out_data, class_34))}\trows with Class 3 & 4 rule"))
# 0	  rows with Class 3 & 4 rule

# some ten_year rulers
cat(read_file("~/persondata/31318/313180000000698"))
# 313180000000698,2016-12-21,G.46.2-878,Infraction,,Prepaid
# 313180000000698,2008-06-26,18.2-250,Felony,,Guilty
# 313180000000698,2009-10-29,19.2-306,Felony,,Sentence/Probation Revoked

cat(read_file("~/persondata/27015/270150000000668"))
# 270150000000668,2016-12-29,B.46.2-301,Misdemeanor,,Nolle Prosequi
# 270150000000668,2000-10-23,18.2-95,Felony,,Sentence/Probation Revoked
# 270150000000668,2000-06-07,18.2-95,Felony,,Resolved
# 270150000000668,2018-01-08,B.46.2-301,Misdemeanor,,Guilty
# 270150000000668,2018-01-08,18.2-323.1,Misdemeanor,,Guilty
# 270150000000668,2018-01-08,46.2-1013,Infraction,,Dismissed
# 270150000000668,2009-10-19,18.2-250,Felony,,Guilty
# 270150000000668,2009-07-14,18.2-250,Felony,,Nolle Prosequi
# 270150000000668,2010-02-08,B.46.2-301,Misdemeanor,,Guilty In Absentia


###################################
# once we get it, we'll run it on everything...
###################################

