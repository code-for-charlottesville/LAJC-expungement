library(dplyr)
library(readr)

chargetype <- c("Misdemeanor", "Felony") # No infraction
disposition <- c("Conviction", "Dismissed", "Deferral Dismissal")
codesection <- c("covered in 19.2-392.6 - A",
                 "covered in 19.2-392.6 - B",
                 "excluded by 19.2-392.12",
                 "covered elsewhere")
arrests <- c(FALSE, TRUE)
convictions <- c(FALSE, TRUE)
felony10 <- c(FALSE, TRUE)
sevenyear <- c(FALSE, TRUE)
tenyear <- c(FALSE, TRUE)
within7 <- c(FALSE, TRUE)
within10 <- c(FALSE, TRUE)
class1_2 <- c(FALSE, TRUE)
class3_4 <- c(FALSE, TRUE)

cases <- expand.grid(chargetype, disposition, codesection, arrests, 
                     convictions, felony10, sevenyear, tenyear, 
                     within7, within10, class1_2, class3_4)

colnames(cases) <- c("chargetype", "disposition", "codesection", "arrests", 
                     "convictions", "felony10", "sevenyear", "tenyear", 
                     "within7", "within10", "class1_2", "class3_4")

cases <- cases %>%
  mutate(disposition = as.factor(cases$disposition),
         codesection = as.factor(cases$codesection),
         chargetype = as.factor(cases$chargetype)) 

# S1: Dismissal of misdemeanor charges, with no arrests or charges in the past 3 years, 
# and with no convictions on the person's record (Automatic)
S1 <- cases$disposition == "Dismissed" & 
  cases$chargetype == "Misdemeanor" & 
  !cases$arrests & 
  !cases$convictions

# S2: Deferred dismissal of misdemeanor charges covered in 6A (Automatic)
S2 <- cases$disposition == "Deferral Dismissal" & 
  cases$chargetype == "Misdemeanor" & 
  cases$codesection == "covered in 19.2-392.6 - A"

# S3: Conviction of misdemeanor charges covered in 19.2-392.6 B with no convictions of 
# any other kind within 7 years from disposition date (Automatic)
S3 <- cases$disposition == "Conviction" & 
  cases$chargetype == "Misdemeanor" & 
  (cases$codesection =="covered in 19.2-392.6 - A" | cases$codesection =="covered in 19.2-392.6 - B") &
  !cases$sevenyear

# S4: Conviction of misdemeanor charges covered in 19.2-392.6 B with no convictions of 
# any kind since the disposition date, but the disposition date is within seven years of 
# the current date (Automatic (pending))
S4 <- cases$disposition == "Conviction" & 
  cases$chargetype == "Misdemeanor" & 
  (cases$codesection =="covered in 19.2-392.6 - A" | cases$codesection =="covered in 19.2-392.6 - B") &
  !cases$sevenyear & 
  cases$within7

# S5: Deferred dismissal of misdemeanor charges not covered under 6A and not excluded 
# under 12 (Petition)
S5 <- cases$disposition == "Deferral Dismissal" & 
  cases$chargetype == "Misdemeanor" & 
  (cases$codesection=="covered elsewhere" | cases$codesection=="covered in 19.2-392.6 - B")

# S6: Dismissal of felony charges (Petition)
S6 <- cases$disposition == "Dismissed" & 
  cases$chargetype == "Felony"

# S7: Dismissal of misdemeanor charges, but with (arrests or charges in the past 3 years 
# OR with convictions on the person's record) (Petition)
S7 <- cases$disposition == "Dismissed" & 
  cases$chargetype == "Misdemeanor" & 
  (cases$arrests | cases$convictions)

# S8: (Conviction OR deferred dismissal) of felony charges not excluded under 19.2-392.12, 
# with no class 3 or 4 felony conviction within the past 20 years, no felony within the past 
# 10 years, no class 1 or 2 felony or any other felony punishable by imprisonment for life, 
# and with no convictions of any other kind within 10 years from disposition date (Petition)
S8 <- (cases$disposition == "Conviction" | cases$disposition == "Deferral Dismissal") & 
  cases$chargetype == "Felony" & 
  cases$codesection!="excluded by 19.2-392.12" & 
  !cases$class3_4 & 
  !cases$felony10 & 
  !cases$class1_2 & 
  !cases$tenyear

# S9: Conviction of misdemeanor charges not covered in 19.2-392.6 B or excluded by 19.2-392.12, 
# with no class 3 or 4 felony conviction within the past 20 years, no felony within the past 10 years, 
# and no class 1 or 2 felony or any other felony punishable by imprisonment for life, 
# and with no convictions of any other kind within 7 years from disposition date (Petition)
S9 <- cases$disposition == "Conviction" & 
  cases$chargetype == "Misdemeanor" & 
  cases$codesection=="covered elsewhere" & 
  !cases$class3_4 & 
  !cases$felony10 & 
  !cases$class1_2 & 
  !cases$sevenyear

# S10: Conviction OR deferred dismissal of felony charges not excluded under 19.2-392.12, 
# with no class 3 or 4 felony conviction within the past 20 years, no felony within the past 10 years, 
# no class 1 or 2 felony or any other felony punishable by imprisonment for life, 
# with no convictions of any kind since the disposition date, but the disposition date is 
# within ten years of the current date (Petition (pending))
S10 <- (cases$disposition == "Conviction" | cases$disposition == "Deferral Dismissal") & 
  cases$chargetype == "Felony" & 
  cases$codesection!="excluded by 19.2-392.12" & 
  !cases$class3_4 & !cases$felony10 & 
  !cases$class1_2 & 
  !cases$tenyear & 
  cases$within10

# S11: Conviction of misdemeanor charges not covered in 19.2-392.6 B or excluded by 19.2-392.12, 
# with no class 3 or 4 felony conviction within the past 20 years, no felony within the past 10 years, 
# and no class 1 or 2 felony or any other felony punishable by imprisonment for life, 
# with no convictions of any kind since the disposition date, 
# but the disposition date is within ten years of the current date (Petition (pending))
S11 <- cases$disposition == "Conviction" & 
  cases$chargetype == "Misdemeanor" & 
  cases$codesection=="covered elsewhere" & 
  !cases$class3_4 & 
  !cases$felony10 & 
  !cases$class1_2 & 
  !cases$sevenyear & 
  cases$within7

# Expungability coded as listed above; Otherwise not eligible
cases$expungability <- "Not eligible"
cases$expungability <- ifelse(S1 | S2 | S3, "Automatic", cases$expungability)
cases$expungability <- ifelse(S4, "Automatic (pending)", cases$expungability)
cases$expungability <- ifelse(S5 | S6 | S7 | S8 | S9, "Petition", cases$expungability)
cases$expungability <- ifelse(S10 | S11, "Petition (pending)", cases$expungability)
cases$expungability <-as.factor(cases$expungability)

cases <- cases %>%
  rename(
    arrest_disqualifier = arrests,
    felony_conviction_disqualifier = felony10,
    next_conviction_disqualifier_after_misdemeanor = sevenyear,
    next_conviction_disqualifier_after_felony = tenyear,
    pending_after_misdemeanor = within7,
    pending_after_felony = within10
  )

write_csv(cases, "./training_set.csv")
