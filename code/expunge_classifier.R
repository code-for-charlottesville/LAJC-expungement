library(randomForest)
library(tidyverse)
library(here)
source(here("code", "helper-functions.R"))

load(here("expunge_coder.Rdata"))

classify_ex <- function(id){
  data <- read_person_file(id)
  
  data <- filter(data, ChargeType %in% c("Felony", "Misdemeanor"))
  
  if(nrow(data)==0) return(data)
  
  A <- c("4.1-305", "18.2-250.1")
  B <- c("4.1-305","18.2-96","18.2-103","18.2-119","18.2-120","18.2-134","18.2-250.1","18.2-415")
  Bmis <- c("18.2-248.1")
  Twelve <- c("18.2-36.1","18.2-36.2","18.2-51.4","18.2-51.5","18.2-57.2","18.2-266","46.2-341.24")
  
  yearfun <- function(data, row, end, y, felony=FALSE){
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
  
  yearfun_petition <- function(data, row, petition, y, felony=FALSE){
    require(lubridate)
    cutoff <- as.Date(petition)  %m-% years(y)
    daterange <- data$HearingDate > cutoff
    df <- data[daterange,]
    sevenyear <- sum(df$DispositionCode == "Guilty") > 0
    if(felony) sevenyear <- sevenyear & (sum(df$ChargeType == "Felony") > 0)
    return(sevenyear)
  }
  
  data$sevenyear <- NA
  data$tenyear <- NA
  data$arrests <- NA
  data$anyfelony <- NA
  data$sevenyearpending <- NA
  data$tenyearpending <- NA
  for(i in 1:nrow(data)){
    data[i,]$sevenyear <- yearfun(data, row=i, end = "2020-01-01", y = 7, felony=FALSE)
    data[i,]$tenyear <- yearfun(data, row=i, end = "2020-01-01", y=10, felony=TRUE) ###### only 19.2-392.12
    data[i,]$sevenyearpending <- yearfun(data, row=i, end = "2070-01-01", y=7, felony=FALSE)
    data[i,]$tenyearpending <- yearfun(data, row=i, end = "2070-01-01", y=10, felony=TRUE)  ###### only 19.2-392.12
    data[i,]$arrests <- yearfun(data, row=i, end = "2020-01-01", y=-3, felony=FALSE) ###### should this actually be from petition date?????
    data[i,]$anyfelony <- yearfun_petition(data, row=i, petition = "2020-01-01", y=10, felony=TRUE)
    data[i,]$anyfelonypending <- yearfun_petition(data, row=i, petition = "2070-01-01", y=10, felony=TRUE)

  }
  
  data <- data %>%
    mutate(chargetype = ChargeType,
           disposition = DispositionCode,
           disposition = as.character(fct_recode(disposition,
                                                 "Dismissed" = "Nolle Prosequi",
                                                 "Conviction" = "Guilty In Absentia",
                                                 "Conviction" = "Guilty",
                                                 "Dismissed" = "Not Guilty")),
           disposition = ifelse(Plea %in% c("Alford", "Guilty", "Nolo Contendere") & disposition == "Dismissed",
                                "Deferral Dismissal", 
                                disposition),
           codesection = "covered elsewhere",
           codesection = ifelse(CodeSection %in% B | CodeSection %in% Bmis, "covered in 19.2-392.6 - B", codesection),
           codesection = ifelse(CodeSection=="4.1-305" & disposition == "Deferral Dismissal", "covered in 19.2-392.6 - A", codesection),
           codesection = ifelse(CodeSection=="18.2-250.1", "covered in 19.2-392.6 - A", codesection),
           codesection = ifelse(CodeSection %in% Twelve, "covered in 19.2-392.12", codesection),
           chargetype = as.factor(chargetype),
           disposition = as.factor(disposition),
           codesection = as.factor(codesection),
           anyconvict = any(disposition == "Guilty"),
           class1_2 = any(Class %in% c("1", "2") & chargetype=="Felony"),
           class1_2 = ifelse(is.na(class1_2), FALSE, class1_2),
           class3_4 = any(Class %in% c("3", "4") & chargetype=="Felony"),
           class3_4 = ifelse(is.na(class3_4), FALSE, class3_4))
  
  data <- filter(data, disposition %in% c("Conviction", "Dismissed", "Deferral Dismissal")) %>%
    mutate(disposition = as.factor(as.character(disposition)))
  
  levels(data$disposition) <- c("Conviction", "Dismissed", "Deferral Dismissal")
  levels(data$codesection) <- c("covered in 19.2-392.6 - A",
                                "covered in 19.2-392.6 - B",
                                "covered in 19.2-392.12",
                                "covered elsewhere")
  levels(data$chargetype) <- c("Misdemeanor", "Felony")

  calculate_expungement <- function(data) {
    data <- data %>%
      mutate(expungable = predict(expunge_coder, newdata=data)) %>%
      group_by(HearingDate) %>%
      mutate(nonauto_count_day = sum(expungable != "Automatic")) %>%
      ungroup() %>%
      arrange(HearingDate) %>%
      mutate(totalexpunge = cumsum(expungable %in% c("Automatic", "Petition")))
    
    data$expungable[data$totalexpunge > 2] <- "Not eligible"
    data$expungable[data$expungable == "Automatic" & data$nonauto_count_day > 1] <- "Petition"
    data
  }
  
  data <- calculate_expungement(data)
  pendingdata <- calculate_expungement(
    mutate(
      data, 
      sevenyear = sevenyearpending, 
      tenyear = tenyearpending,
      anyfelony = anyfelonypending
    )
  )
  
  data <- data %>%
    mutate(
      expungable_pending = pull(pendingdata, expungable),
      old_expunge = (disposition != "Conviction")
    )
  
  return(data)
}
