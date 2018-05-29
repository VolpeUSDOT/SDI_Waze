# Data prep for Tableau viz

# setup ----
library(aws.s3)
library(tidyverse)

codeloc <- "~/SDI_Waze" 
# Set grid size:
HEXSIZE = c("1", "4", "05")[1] # Change the value in the bracket to use 1, 4, or 0.5 sq mi hexagon grids
do.months = c("04","05","06","07","08","09")

inputdir <- "WazeEDT_RData_Input"
outputdir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_RandForest_Output")

aws.signature::use_credentials()
waze.bucket <- "ata-waze"
localdir <- "/home/dflynn-volpe/workingdata" 

setwd(localdir)
# read utility functions, including prep.hex and append.hex
source(file.path(codeloc, 'utility/wazefunctions.R'))

for(mo in do.months){
  prep.hex(file.path(inputdir, paste0("WazeTimeEdtHexWx_", mo, "_", HEXSIZE, "_mi.RData")), month = mo)
}

# Add FARS, AADT, HPMS, jobs
na.action = "fill0"
for(w in c("w.04", "w.05", "w.06", "w.07","w.08", "w.09")){
  append.hex(hexname = w, data.to.add = "FARS_MD_2012_2016_sum_annual", na.action = na.action)
  append.hex(hexname = w, data.to.add = "hexagons_1mi_routes_AADT_total_sum", na.action = na.action)
  append.hex(hexname = w, data.to.add = "hexagons_1mi_routes_sum", na.action = na.action)
  append.hex(hexname = w, data.to.add = "hexagons_1mi_bg_lodes_sum", na.action = na.action)
  append.hex(hexname = w, data.to.add = "hexagons_1mi_bg_rac_sum", na.action = na.action)
}

w.04_09 <- rbind(w.04, w.05, w.06, w.07, w.08, w.09)

# !!! Filter for days with good EDT data, meaning day > 259 in our initial sample.
w.04_09 = w.04_09 %>% filter(as.numeric(day) < 259) 

avail.cores = parallel::detectCores()

# Omit as predictors in this vector:
alwaysomit = c(grep("GRID_ID", names(w.04), value = T), "day", "hextime", "year", "weekday", 
               "uniqWazeEvents", "nWazeRowsInMatch", 
               "nMatchWaze_buffer", "nNoMatchWaze_buffer",
               grep("EDT", names(w.04), value = T))

alert_types = c("nWazeAccident", "nWazeJam", "nWazeRoadClosed", "nWazeWeatherOrHazard")

alert_subtypes = c("nHazardOnRoad", "nHazardOnShoulder" ,"nHazardWeather", "nWazeAccidentMajor", "nWazeAccidentMinor", "nWazeHazardCarStoppedRoad", "nWazeHazardCarStoppedShoulder", "nWazeHazardConstruction", "nWazeHazardObjectOnRoad", "nWazeHazardPotholeOnRoad", "nWazeHazardRoadKillOnRoad", "nWazeJamModerate", "nWazeJamHeavy" ,"nWazeJamStandStill",  "nWazeWeatherFlood", "nWazeWeatherFog", "nWazeHazardIceRoad")

response.var = "MatchEDT_buffer_Acc"

# 1. Waze data, April - September 2017 Maryland ----
# Based on w.04_09 data frame from RandomForest_WazeGrid_Full.R
# Output as Waze_04-09_GridCounts.csv
# add counts of EDT damage and EDT injury (no, all 0 actually)

# needed: 

dim(w.04_09)

# output for visualization
w.out.for.viz <- w.04_09[c("GRID_ID", "day","hour","DayOfWeek",
                           "nMatchEDT_buffer_Acc",
                           "MatchEDT_buffer_Acc",
                           "nWazeAccident","nWazeAccidentMajor","nWazeAccidentMinor",
                           "uniqEDTreports",
                           "uniqWazeEvents",
                           "nEDTMaxDamDisabling")]

dim(w.out.for.viz)

write.csv(w.out.for.viz, file = "Waze_04-09_GridCounts.csv", row.names = F)

# Also output additional variables

# outputs supplemental variables by GRID_ID for Tableau work
outputvars = c("GRID_ID",
               c("CRASH_SUM", "FATALS_SUM"), # FARS variables, 
               grep("F_SYSTEM", names(w.04), value = T), # road class
               c("MEAN_AADT", "SUM_AADT", "SUM_miles"), # AADT
               grep("WAC", names(w.04), value = T), # Jobs workplace
               grep("RAC", names(w.04), value = T) # Jobs residential
)

w.supp = w.04[!duplicated(w.04$GRID_ID), outputvars]
write.csv(w.supp, file = "Waze_GridCounts_Supplemental.csv", row.names = F)

dim(w.supp)
for(i in names(w.04[,outputvars])) cat(i, "\n")

# 2. EDT data, April - mid September 2017 Maryland ----
# Grid ID day hour dayofweek nEDT, fatal etc
# 2M 8 col

# 3. Model 30 outputs, April - September 2017 Maryland ----
# All_Model_30.csv

modelno = "30"
omits = c(alwaysomit, alert_subtypes)

s3load(object = file.path(paste(outputdir, sep="_"), paste("Model", modelno, "RandomForest_Output.RData", sep= "_")), bucket = waze.bucket)

fitvars <- names(w.04_09)[is.na(match(names(w.04_09), omits))]

head(out.df)

# Re-fit with new cutoff of 0.28
rf.pred <- predict(rf.out, w.04_09[testrows, fitvars], type = "response", cutoff = c(0.775, 0.225))
levels(rf.pred) = c("NoCrash","Crash")
rf.prob <- predict(rf.out, test.dat.use[fitvars], type = "prob", cutoff =  c(0.775, 0.225))
reference.vec <- test.dat.use[,response.var]

class(reference.vec) <- "data.frame"
reference.vec <- as.factor(reference.vec[,1])
levels(reference.vec) = c("NoCrash", "Crash")
out.df <- data.frame(test.dat.use[, c("GRID_ID", "day", "hour", response.var)], rf.pred, rf.prob)
out.df$day <- as.numeric(out.df$day)
names(out.df)[4:7] <- c("Obs", "Pred", "Prob.Noncrash", "Prob.Crash")
out.df = data.frame(out.df,
                    TN = out.df$Obs == 0 &  out.df$Pred == "NoCrash",
                    FP = out.df$Obs == 0 &  out.df$Pred == "Crash",
                    FN = out.df$Obs == 1 &  out.df$Pred == "NoCrash",
                    TP = out.df$Obs == 1 &  out.df$Pred == "Crash")




w.test <- w.04_09[testrows,]

identical(w.test$GRID_ID, out.df$GRID_ID)

w.group <- data.frame(TN = out.df$TN, TP = out.df$TP, FP = out.df$FP, FN = out.df$FN)

w.group$TN[w.group$TN==TRUE] = "TN"
w.group$TP[w.group$TP==TRUE] = "TP"
w.group$FP[w.group$FP==TRUE] = "FP"
w.group$FN[w.group$FN==TRUE] = "FN"

w.group$group <- apply(w.group, 1, function(x) x[x!=FALSE][1])

grp  <- as.factor(w.group$group)

#### Performance by time of day plot
out.df$day <- as.character(out.df$day)

dd <- data.frame(out.df, Pred.grp = grp, w.test[c("nMatchEDT_buffer_Acc", fitvars)]) 

dim(dd)

d2 <- dd %>% 
  group_by(hour) %>%
  summarize(N = n(),
            TotalWazeAcc = sum(nWazeAccident),
            TotalObservedEDT = sum(nMatchEDT_buffer_Acc),
            TotalEstimated = sum(Pred == "Crash"),
            Obs_Est_diff = TotalObservedEDT - TotalEstimated,
            Pct_Obs_Est = 100 *TotalEstimated / TotalObservedEDT)

ggplot(d2, aes(x = hour, y = Pct_Obs_Est)) + geom_line() + ylim(c(50, 120)) +
  ylab("Percent of Observed EDT crashes Estimated")

paste(rep(seq(0, 12, 2), 2), c("AM", "PM"))
labs <- c("12 AM", "2 AM", "4 AM", "6 AM", "8 AM", "10 AM",
          "12 PM", "2 PM", "4 PM", "6 PM", "8 PM", "10 PM")

ggplot(d2, aes(x= hour, y= Pct_Obs_Est, fill = TotalWazeAcc)) +
  geom_line(aes(y = 100, x = 0:23), lwd = 1.5, col = "darkgreen") +
  geom_bar(stat="identity")+
  coord_polar()+
  ylim(c(0, 120)) + 
  ylab("") +
  xlab("Hour of Day") + 
  scale_y_continuous(labels = "", breaks = 1) +
  scale_x_continuous(labels = labs,
                     breaks= seq(0, 23, 2)) +
  ggtitle("Model 30: Estimated EDT crashes / observed \n By hour of day")

write.csv(d2, "Obs_Est_EDT_Model_30_by_hour.csv", row.names = F)
write.csv(dd, "All_Model_30.csv", row.names = F)

# Average by day

# remake day of week, something went wrong -- should not have 0 and 7 both.
datetime <- strptime(paste("2017", dd$day, sep = "-"), format = "%Y-%j")
dd$DayOfWeek <- as.numeric(format(datetime, "%w")) # Weekday as decimal number (0–6, Sunday is 0).
dd$Month <- as.character(format(datetime, "%B"))  # Full month name

d.day <- dd %>% 
  group_by(DayOfWeek) %>%
  summarize(N = n(),
            TotalWazeAcc = sum(nWazeAccident),
            TotalObservedEDT = sum(nMatchEDT_buffer_Acc),
            TotalEstimated = sum(Pred == "Crash"),
            Obs_Est_diff = TotalObservedEDT - TotalEstimated,
            Pct_Obs_Est = 100 *TotalEstimated / TotalObservedEDT)

d.day

keepcol = c("GRID_ID",
            "day",
            "hour",
            "Month",
            "DayOfWeek",
            "Obs",
            "Pred",
            "Prob.Crash",
            "FN",
            "FP",
            "TN",
            "TP",
            "Pred.grp",
            "nMatchEDT_buffer_Acc",
            "nWazeAccident")

all(keepcol %in% names(dd))

write.csv(dd[keepcol], "Subset2_Model_30.csv", row.names = F)

dd$Pred_sum = 1 # to match previous Tableau prep

keepcol = c("GRID_ID",
            "day",
            "hour",
            "DayOfWeek",
            "Pred",
            "Pred_sum",
            "nMatchEDT_buffer_Acc",
            "Month")

write.csv(dd[keepcol], "Subset2_Model_30_byMONTH.csv", row.names = F)
