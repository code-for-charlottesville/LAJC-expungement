chargetype <- c("Misdemeanor", "Felony") # No infraction
disposition <- c("Conviction", "Dismissed", "Deferral Dismissal")
codesection <- c("covered in 19.2-392.6 - A",
                 "covered in 19.2-392.6 - B",
                 "covered in 19.2-392.12",
                 "covered elsewhere")
sevenyear <- c(FALSE, TRUE)
tenyear <- c(FALSE, TRUE)
sameday <- c(FALSE, TRUE)
lifetime <- c(FALSE, TRUE)
anyconvict <- c(FALSE, TRUE)
arrests <- c(FALSE, TRUE)
class1_2 <- c(FALSE, TRUE)
class3_4 <- c(FALSE, TRUE)
anyfelony <- c(FALSE, TRUE)

cases <- expand.grid(chargetype, disposition,codesection,
                     sevenyear, tenyear, sameday, lifetime,
                     anyconvict, arrests, class1_2,
                     class3_4, anyfelony)

colnames(cases) <- c("chargetype", "disposition","codesection",
                     "sevenyear", "tenyear", "sameday", "lifetime",
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
     !cases$sevenyear & !cases$class1_2 & !cases$class3_4 & !cases$anyfelony & !cases$sameday &
     (cases$codesection %in% c("covered in 19.2-392.6 - A", "covered in 19.2-392.6 - B"))
cases$expungability[S3] <- "Automatic"

S4 <- cases$chargetype == "Misdemeanor" &
     cases$disposition == "Conviction" &
     !cases$sevenyear & !cases$class1_2 & !cases$class3_4 & !cases$anyfelony & !cases$sameday &
     cases$codesection == "covered elsewhere"
cases$expungability[S4] <- "Petition"

S5 <- cases$chargetype == "Misdemeanor" &
     cases$disposition == "Conviction" &
     (cases$sevenyear | cases$class1_2 | cases$class3_4 | cases$anyfelony | cases$sameday |
           cases$codesection == "covered in 19.2-392.12")
cases$expungability[S5] <- "Not eligable"

S6 <- cases$chargetype == "Felony" &
     cases$codesection != "covered in 19.2-392.12" & #is this right??
     !cases$tenyear & !cases$class1_2 & !cases$class3_4 & !cases$anyfelony & !cases$sameday
cases$expungability[S6] <- "Petition"

S7 <- cases$chargetype == "Felony" &
     (cases$codesection == "covered in 19.2-392.12" | #is this right?? |
           cases$tenyear | cases$class1_2 | cases$class3_4 |
           cases$anyfelony | cases$sameday)
cases$expungability[S7] <- "Not eligable"

