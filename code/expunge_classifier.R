setwd("~/Box Sync/Code for Cville")

library(randomForest)
library(tidyverse)
library(here)
source(here("code", "helper-functions.R"))
NODE_ENCODE <- readr::read_csv(here("data", "reasons_encode.csv"), col_types = "ic")

########## Load data and trained classifier ##########

classify_ex <- function(id){
  
  data <- read_person_file(id) %>%
    replace_na(list(CodeSection = "MISSING"))
  load(here("data", "expunge_coder.Rdata"))
  orig_cols <- colnames(data)
  
  ########## Data cleaning ##########
  
  data <- data %>% 
    filter(!is.na(DispositionCode)) %>%
    filter(ChargeType %in% c("Misdemeanor", "Felony")) %>%
    filter(
      DispositionCode %in% c(
        "Guilty",
        "Guilty In Absentia",
        "Dismissed",
        "Nolle Prosequi",
        "Not Guilty",
        "Not Guilty/Acquitted",
        "No Indictment Presented",
        "Not True Bill",
        # "Resolved", #??
        "Dismissed/Other"
      )
    )
  
  if(nrow(data) == 0) return(data)
  
  ########## Generate objects to assist in feature construction ########
  
  A <- c("4.1-305", "18.2-250.1")
  B <- c("4.1-305","18.2-96","18.2-103","18.2-119","18.2-120","18.2-134","18.2-250.1","18.2-415")
  Bmis <- c("18.2-248.1")
  Twelve <- c("18.2-36.1","18.2-36.2","18.2-51.4","18.2-51.5","18.2-57.2","18.2-266","46.2-341.24")
  
  #Generate time-distance matrices
  dates <- as.numeric(as.Date(data$HearingDate))
  d <- outer(dates, dates, "-") # difference in days between all dates
  
  d_fel <- d # isolate dates within the last ten years
  d_fel[d > 365.25*10 | d < 0] <- 0
  d_fel[d_fel != 0] <- 1
  
  d_arr <- d # isolate dates within the last three years
  d_arr[d > 365.25*3 | d < 0] <- 0
  d_arr[d_arr != 0] <- 1
  
  d7 <- d # isolate dates within the next seven years
  d7[d > 0 | d < -365.25*7] <- 0
  d7[d7 != 0] <- 1
  
  d10 <- d # isolate dates within the next ten years
  d10[d > 0 | d < -365.25*10] <- 0
  d10[d10 != 0] <- 1
  
  ########## FEATURE CONSTRUCTION ##########
  
  # Disposition = {Convicted; Deferred Dismissal; Dismissed}
  # Deferred dismissal if disposition = Dismissed and plea = Guilty
  # Plea = {guilty; not guilty}
  # Must combine nolle prosequi and other pleas into these two categories
  
  data <- data %>%
    mutate(disposition = DispositionCode,
           disposition = fct_recode(disposition,
                                    "Dismissed" = "Nolle Prosequi",
                                    "Dismissed" = "No Indictment Presented",
                                    "Dismissed" = "Not True Bill",
                                    "Dismissed" = "Dismissed/Other",
                                    "Dismissed" = "Not Guilty",
                                    "Dismissed" = "Not Guilty/Acquitted",
                                    "Conviction" = "Guilty In Absentia",
                                    "Conviction" = "Guilty"),
           disposition = fct_expand(disposition, "Conviction", "Dismissed", "Deferral Dismissal")
    )
  dd <- data$Plea %in% c("Alford", "Guilty", "Nolo Contendere") & data$disposition == "Dismissed"
  data$disposition[dd] <- "Deferral Dismissal"
  
  # Chargetype = {Felony; Misdemeanor}
  
  data <- data %>%
    mutate(chargetype = factor(ChargeType, levels = c("Misdemeanor", "Felony")),
           chargetype = fct_expand(chargetype, "Misdemeanor", "Felony"))
  
  # Codesection = {covered in 19.2-392.6 A; covered in 19.2-392.6 B; excluded by 19.2-392.12; covered elsewhere}
  
  data <- data %>%
    mutate(codesection = "covered elsewhere",
           codesection = ifelse(CodeSection %in% B | (CodeSection %in% Bmis & chargetype == "Misdemeanor"), "covered in 19.2-392.6 - B", codesection),
           codesection = ifelse(CodeSection %in% A & disposition == "Deferral Dismissal", "covered in 19.2-392.6 - A", codesection),
           codesection = ifelse(CodeSection %in% Twelve, "excluded by 19.2-392.12", codesection),
           codesection = as.factor(codesection),
           codesection = fct_expand(codesection, "covered in 19.2-392.6 - A",
                                    "covered in 19.2-392.6 - B",
                                    "excluded by 19.2-392.12",
                                    "covered elsewhere"))
  
  # Convictions = {True; False} (convictions on the person's record)
  
  data <- data %>%
    mutate(convictions = any(disposition == "Conviction"))
  
  # Arrests = {True; False} (arrests or charges in the past 3 years)
  
  data <- data %>%
    mutate(arrests = (rowSums(d_arr)!=0))
  
  # Felony10 = {True;False} (felony convictions within the last 10 years)
  
  data <- data %>%
    mutate(felony10 = as.logical(d_fel %*% (chargetype == 'Felony' & disposition == "Conviction")) > 0)
  
  # Sevenyear = {True; False} (convictions of another kind within 7 years from disposition date)
  # Tenyear = {True; False} (convictions of another kind within 10 years from disposition date)
  
  data <- data %>%
    mutate(sevenyear = as.logical(d7 %*% as.matrix(disposition == "Conviction")) > 0,
           tenyear = as.logical(d10 %*% as.matrix(disposition == "Conviction")) > 0)
  
  # Within7 = {True; False} (disposition date is within 7 years of the current date)
  # Within10 = {True; False} (disposition date is within 10 years of the current date)
  
  data <- data %>%
    mutate(HearingDate = as.Date(HearingDate),
           within7 = (HearingDate > (as.Date("2020-12-31") - lubridate::years(7))),
           within10 = (HearingDate > (as.Date("2020-12-31") - lubridate::years(10))))
  
  # Class3_4 = {True; False} (class 3 or 4 felony conviction within the past 20 years)
  # Class1_2 = {True; False} (class 1 or 2 felony or any other felony punishable by imprisonment for life)
  
  data <- data %>%
    mutate(class1_2 = any(Class %in% c("1", "2") & chargetype=="Felony"),
           class1_2 = ifelse(is.na(class1_2), FALSE, class1_2),
           class3_4 = any(Class %in% c("3", "4") & chargetype=="Felony"),
           class3_4 = ifelse(is.na(class3_4), FALSE, class3_4))
  
  ############## Apply the Random Forest to perform classification here ############
  
  # apply the auto encoder:
  data <- data %>%
    mutate(expungable = predict(expunge_coder, newdata=data))
  
  # add old law expungable count
  data <- data %>% 
    mutate(old_expungable = (disposition == "Dismissed"))
  
  # rationales
  tree <- getTree(expunge_coder, labelVar=TRUE) #tree
  nodes <- attr(predict(expunge_coder, newdata=data, nodes=TRUE), "nodes")
  data$node <- nodes
  data <- left_join(data, NODE_ENCODE, by="node")
  
  ############# Additional conditions that change the expungement outcome ##########
  
  # Sameday = {True; False} (same day rule in effect)
  
  data <- data %>%
    group_by(HearingDate) %>%
    mutate(nonauto_count_day = sum(expungable != "Automatic" & expungable != "Automatic (pending)")) %>%
    ungroup() %>%
    mutate(sameday = (expungable %in% c("Automatic", "Automatic (pending)") & nonauto_count_day > 0),
           reason2 = ifelse(sameday, "; HOWEVER, the outcome is changed to petition because of a conviction on the same day of something that is not automatically expungable",""))
  
  data$expungable[data$sameday & data$expungable=="Automatic"] <- "Petition"
  data$expungable[data$sameday & data$expungable=="Automatic (pending)"] <- "Petition (pending)"
  
  # Lifetime = {True; False} (more than 2 lifetime expungements)
  
  data <- data %>%
    arrange(HearingDate) %>%
    mutate(totalexpunge = cumsum(expungable %in% c("Automatic", "Petition")),
           lifetime = totalexpunge > 2,
           reason2 = ifelse(lifetime & expungable != "Not eligible", "; HOWEVER, the outcome is changed to not eligible because the lifetime limit of two expungements has been exceeded","")) %>%
    unite(reason, reason, reason2, sep="")
  
  data$expungable[data$lifetime] <- "Not eligible"
  
  # Fix missing values for race and sex: replace with mode
  data$Race[is.na(data$Race)] <- names(which.max(table(data$Race)))[1]
  data$Sex[is.na(data$Sex)] <- names(which.max(table(data$Sex)))[1]
  
  ########### Select columns to keep ###############
  data <- dplyr::select(data, person_id, HearingDate, CodeSection, codesection,
                        ChargeType, chargetype, Class, DispositionCode, disposition,
                        Plea, Race, Sex, fips, convictions:old_expungable, reason,
                        sameday, lifetime)
  return(data)
}
