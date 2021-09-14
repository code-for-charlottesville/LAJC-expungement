library(randomForest)
library(tidyverse)
set.seed(22903)

chargetype <- c("Misdemeanor", "Felony") # No infraction
disposition <- c("Conviction", "Dismissed", "Deferral Dismissal")
codesection <- c("covered in 19.2-392.6 - A",
                 "covered in 19.2-392.6 - B",
                 "excluded by 19.2-392.12",
                 "covered elsewhere")
sevenyear <- c(FALSE, TRUE)
anyconvict <- c(FALSE, TRUE)
arrests <- c(FALSE, TRUE)
class1_2 <- c(FALSE, TRUE)
class3_4 <- c(FALSE, TRUE)
anyfelony <- c(FALSE, TRUE) # 10 years

cases <- expand.grid(chargetype, disposition,codesection,
                     sevenyear,
                     anyconvict, arrests, class1_2,
                     class3_4, anyfelony)

colnames(cases) <- c("chargetype", "disposition","codesection",
                     "sevenyear", 
                     "anyconvict", "arrests", "class1_2",
                     "class3_4", "anyfelony")

cases$expungability <- NA

S1 <- cases$chargetype == "Misdemeanor" &
  cases$disposition == "Dismissed" &
  !cases$anyconvict & !cases$arrests
cases$expungability[S1] <- "Automatic"

S2 <- cases$chargetype == "Misdemeanor" &
  cases$disposition != "Conviction" &
  (cases$disposition == "Deferral Dismissal" | cases$anyconvict | cases$arrests)
cases$expungability[S2] <- "Petition"

S3 <- cases$chargetype == "Misdemeanor" &
  cases$disposition == "Conviction" &
  !cases$sevenyear & !cases$class1_2 & !cases$class3_4 & !cases$anyfelony &
  (cases$codesection %in% c("covered in 19.2-392.6 - A", "covered in 19.2-392.6 - B"))
cases$expungability[S3] <- "Automatic"

S4 <- cases$chargetype == "Misdemeanor" &
  cases$disposition == "Conviction" &
  !cases$sevenyear & !cases$class1_2 & !cases$class3_4 & !cases$anyfelony &
  cases$codesection == "covered elsewhere"
cases$expungability[S4] <- "Petition"

S5 <- cases$chargetype == "Misdemeanor" &
  cases$disposition == "Conviction" &
  (cases$sevenyear | cases$class1_2 | cases$class3_4 | cases$anyfelony |
     cases$codesection == "excluded by 19.2-392.12")
cases$expungability[S5] <- "Not eligible"

cases$expungability[cases$chargetype == "Felony"] <- "Petition" # unless specified below

S6 <- cases$chargetype == "Felony" &
  cases$disposition != "Dismissed" &
  (cases$codesection == "excluded by 19.2-392.12" | cases$class1_2 | cases$class3_4 | cases$anyfelony)
cases$expungability[S6] <- "Not eligible"

cases <- cases %>%
  mutate(expungability = as.factor(expungability),
         disposition = as.factor(disposition),
         codesection = as.factor(codesection),
         chargetype = as.factor(chargetype))

expunge_coder <- randomForest(
  formula = expungability ~ .,
  data = cases,
  importance = TRUE,
  mtry = 8,
  ntree = 1
)

### comment out ###
pdf("~/Box Sync/Code for Cville/tree.pdf", width=60, height=8)
reprtree:::plot.getTree(expunge_coder)
dev.off()
getTree(expunge_coder, labelVar = TRUE)
###################

table(cases$expungability, predict(expunge_coder))

save(expunge_coder, file="expunge_coder.Rdata")
