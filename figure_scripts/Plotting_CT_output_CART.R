# CT output prep and plotting

library(tidyverse)

setwd("//vntscex.local/DFS/Projects/PROJ-OS62A1/SDI Waze Phase 2/Output/Random_Forest_Output")

# load("CT_Model_18_RandomForest_Output.RData")
load("MD_Model_30_RandomForest_Output.RData")

load("Model_18_Output_to_CT.RData")

w.group <- data.frame(TN = out.df$TN, TP = out.df$TP, FP = out.df$FP, FN = out.df$FN)

w.group$TN[w.group$TN==TRUE] = "TN"
w.group$TP[w.group$TP==TRUE] = "TP"
w.group$FP[w.group$FP==TRUE] = "FP"
w.group$FN[w.group$FN==TRUE] = "FN"

w.group$group <- apply(w.group, 1, function(x) x[x!=FALSE][1])

grp  <- as.factor(w.group$group)

#### Performance by time of day plot

out.df <- data.frame(out.df, Pred.grp = grp)

out.df$Pred = ifelse(out.df$Pred=="Crash", 1, 0)

out.df$DayOfWeek = lubridate::wday(strptime(paste0("2017-", formatC(out.df$day, width = 3, flag = "0")), "%Y-%j"))

out.df$day <- as.character(out.df$day)

out.df$Hour <- strptime(paste0("2017-", formatC(out.df$day, width = 3, flag = "0"), " ", out.df$hour), "%Y-%j %H")

write.csv(out.df, file = "CT_Model_18_30pct.csv", row.names=F)

### Factoids 

setwd("//vntscex.local/DFS/Projects/PROJ-OS62A1/SDI Waze Phase 2/Output/Random_Forest_Output/Multi-State/")

d <- read.csv("CT_All_Model_30.csv")

# How many total crashes?

d %>%
#  group_by(DayOfWeek) %>%
  summarise(sum(nMatchEDT_buffer_Acc),
            sum(nWazeAccident),
            sum(Pred),
            sum(FATALS_SUM))

d %>%
  group_by(DayOfWeek) %>%
  summarise(Obs = sum(nMatchEDT_buffer_Acc),
            Pred = sum(Pred),
            Pct.obs = Pred / Obs)

d %>%
#  group_by(DayOfWeek) %>%
  summarise(count = n(),
            Obs = sum(nMatchEDT_buffer_Acc),
            Waze.but.no.Crash = count - Obs)

summary(lm1 <- lm(Prob.Crash ~ 
             HOURLY_MAX_AADT_1 +
             HOURLY_MAX_AADT_2 +
             HOURLY_MAX_AADT_3 +
             HOURLY_MAX_AADT_4 +
             HOURLY_MAX_AADT_5,
           data = d
             ))

coef(lm1)


alwaysomit = c(grep("GRID_ID", names(d), value = T), "day", "hextime", "year", "weekday", "vmt_time",
               "uniqWazeEvents", "nWazeRowsInMatch", 
               "nMatchWaze_buffer", "nNoMatchWaze_buffer",
               grep("EDT", names(d), value = T))

alsoomit = c("Obs", "Pred", "Prob.Noncrash", "Prob.Crash","TN","FP","FN","TP",
             "Pred.grp", "hour.1", "Hour")

predvars = names(d[!names(d) %in% c(alwaysomit, alsoomit)])

# Produces a large lm object, ~ 1 Gb
summary(lm.full <- lm(as.formula(paste("Prob.Crash ~", paste(predvars, collapse = " + "))), data = d
))

coef.lm <- data.frame(vars = names(coef(lm.full)), coefs = coef(lm.full))
coef.lm = coef.lm[order(abs(coef.lm$coef), decreasing = T),]
knitr::kable(coef.lm)


### CART Plotting

# Count of crashes observed
sum(d$nMatchEDT_buffer_Acc)

wazeformula <- reformulate(termlabels = predvars, response = 'Pred')

ct.w.5 = party::ctree(wazeformula,
                      data = d,
                      controls = party::ctree_control(maxdepth = 5))

ct.w.6 = party::ctree(wazeformula,
                      data = d,
                      controls = party::ctree_control(maxdepth = 6))

ct.w.10 = party::ctree(wazeformula,
                      data = d,
                      controls = party::ctree_control(maxdepth = 10))


ct.w.8 = party::ctree(wazeformula,
                       data = d,
                       controls = party::ctree_control(maxdepth = 8))

pdf(file.path("~/Temp Working Docs/SDI_temp", "Regression_Tree_Plots.pdf"),
    width = 20, height = 15)


plot(ct.w.5, main="Waze Model 30 CT Variable Exploration",
     type = 'simple',
     cex = 0.6)

plot(ct.w.6, main="Waze Model 30 CT Variable Exploration",
     type = 'simple',
     cex = 0.5)


plot(ct.w.8, main="Waze Model 30 CT Variable Exploration",
     type = 'simple',
     cex = 0.4)

plot(ct.w.10, main="Waze Model 30 CT Variable Exploration",
     type = 'simple',
     cex = 0.4)

# ct.w = party::ctree(wazeformula,
#                     data = d,
#                     controls = party::ctree_control(maxdepth = 10))
# 
# plot(ct.w, main="Waze Model 30 CT Variable Exploration",
#      type = 'simple')

dev.off()
# system(paste("open ", shQuote(file.path("~/Temp Working Docs/SDI_temp", "Regression_Tree_Plots.pdf"))))

