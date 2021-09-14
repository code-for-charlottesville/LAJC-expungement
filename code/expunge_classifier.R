library(randomForest)
library(tidyverse)
library(here)
source(here("code", "helper-functions.R"))
NODE_ENCODE <- readr::read_csv(here("data", "reasons_encode.csv"), col_types = "ic")

#' Classifier helper function for parsing year-related rules
yearfun <- function(data, row, end = "2020-12-31", y=7, felony=FALSE){
  require(lubridate)
  date <- data[row,]$HearingDate
  enddate <- data[row,]$HearingDate %m+% years(y)
  if(enddate > as.Date(end)){
    sevenyear <- TRUE
  } else {
    if(y>0) daterange <- data$HearingDate > date & data$HearingDate <= enddate
    if(y<0) daterange <- data$HearingDate <= date & data$HearingDate > enddate
    df <- data[daterange,]
    sevenyear <- sum(df$DispositionCode == "Guilty") > 0
    if(felony) sevenyear <- sevenyear & (sum(df$ChargeType == "Felony") > 0)
  }
  return(sevenyear)
}

#' The classifier
classify_ex <- function(id){
  
  data <- read_person_file(id) %>%
    replace_na(list(CodeSection = "MISSING"))
  
  load(here("data", "expunge_coder.Rdata"))
  
  orig_cols <- colnames(data)
  
  # exclude some rows based on LAJC feedback
  data <- data %>%
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
  
  A <- c("4.1-305", "18.2-250.1")
  B <- c("4.1-305","18.2-96","18.2-103","18.2-119","18.2-120","18.2-134","18.2-250.1","18.2-415")
  Bmis <- c("18.2-248.1")
  Twelve <- c("18.2-36.1","18.2-36.2","18.2-51.4","18.2-51.5","18.2-57.2","18.2-266","46.2-341.24")
  
  data$sevenyear <- NA
  data$tenyear <- NA
  data$arrests <- NA
  data$anyfelony <- NA
  for(i in 1:nrow(data)){
    data[i,]$sevenyear <- yearfun(data, row=i, felony=FALSE)
    data[i,]$tenyear <- yearfun(data, row=i, y=10, felony=TRUE) 
    data[i,]$arrests <- yearfun(data, row=i, y=-3, felony=FALSE) 
    data[i,]$anyfelony <- yearfun(data, row=i, y=-10, felony=TRUE)
  }
  
  data <- data %>%
    mutate(chargetype = ChargeType,
           disposition = DispositionCode,
           disposition = as.character(fct_recode(disposition,
                                                 "Dismissed" = "Nolle Prosequi",
                                                 "Dismissed" = "No Indictment Presented",
                                                 "Dismissed" = "Not True Bill",
                                                 "Dismissed" = "Dismissed/Other",
                                                 "Dismissed" = "Not Guilty",
                                                 "Dismissed" = "Not Guilty/Acquitted",
                                                 # "Dismissed" = "Resolved", #??
                                                 "Conviction" = "Guilty In Absentia",
                                                 "Conviction" = "Guilty"
           )),
           disposition = ifelse(Plea %in% c("Alford", "Guilty", "Nolo Contendere") & disposition == "Dismissed",
                                "Deferral Dismissal", 
                                disposition),
           codesection = "covered elsewhere",
           codesection = ifelse(CodeSection %in% B | CodeSection %in% Bmis, "covered in 19.2-392.6 - B", codesection),
           codesection = ifelse(CodeSection=="4.1-305" & disposition == "Deferral Dismissal", "covered in 19.2-392.6 - A", codesection),
           codesection = ifelse(CodeSection=="18.2-250.1", "covered in 19.2-392.6 - A", codesection),
           codesection = ifelse(CodeSection %in% Twelve, "excluded by 19.2-392.12", codesection),
           chargetype = factor(chargetype, levels = c("Misdemeanor", "Felony")),
           disposition = factor(disposition, levels = c("Conviction", "Dismissed", "Deferral Dismissal")),
           codesection = factor(codesection, levels = c("covered in 19.2-392.6 - A",
                                                        "covered in 19.2-392.6 - B",
                                                        "excluded by 19.2-392.12",
                                                        "covered elsewhere")),
           anyconvict = any(disposition == "Conviction"),
           class1_2 = any(Class %in% c("1", "2") & chargetype=="Felony"),
           class1_2 = ifelse(is.na(class1_2), FALSE, class1_2),
           class3_4 = any(Class %in% c("3", "4") & chargetype=="Felony"),
           class3_4 = ifelse(is.na(class3_4), FALSE, class3_4))
  
  data <- data %>%
    mutate(expungable = predict(expunge_coder, newdata=data)) %>%
    group_by(HearingDate) %>%
    mutate(nonauto_count_day = sum(expungable != "Automatic")) %>%
    ungroup() %>%
    arrange(HearingDate) %>%
    mutate(totalexpunge = cumsum(expungable %in% c("Automatic", "Petition")))
  
  data$expungable[data$totalexpunge > 2] <- "Not eligible"
  data$expungable[data$expungable == "Automatic" & data$nonauto_count_day > 1] <- "Petition"
  
  levels(data$expungable) <- c(levels(data$expungable), 
                               "Automatic (pending)",
                               "Petition (pending)")
  
  # Coding people who will be eligible, but aren't yet
  data2 <- data
  data2$sevenyear <- FALSE
  data2$tenyear <- FALSE
  data2 <- data2 %>%
    mutate(expungable = predict(expunge_coder, newdata=data2)) 
  
  autpend <- (data$expungable=="Not eligible" & data2$expungable=="Automatic")
  petpend <- (data$expungable=="Not eligible" & data2$expungable=="Petition")
  data$expungable[autpend] <- "Automatic (pending)"
  data$expungable[petpend] <- "Petition (pending)"
  
  # add old law expungable count
  data <- data %>% 
    mutate(old_expungable = (disposition != "Conviction"))
  
  #Rationales
  tree <- getTree(expunge_coder, labelVar=TRUE) #tree
  nodes <- attr(predict(expunge_coder, newdata=data, nodes=TRUE), "nodes")
  data$node <- nodes
  data <- left_join(data, NODE_ENCODE, by="node")
  data <- dplyr::select(data, all_of(orig_cols), expungable, reason, old_expungable)
  return(data)
}