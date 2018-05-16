# testing model diagnostics by road type
library(tidyverse)
setwd("//vntscex.local/DFS/Projects/PROJ-OR02A2/SDI/Model_Output")

load("RandomForest_Output_04_AccMatch.RData")

codedir <- "~/git/SDI_Waze" 
source(file.path(codedir, "utility/wazefunctions.R")) # for movefiles() function

# match w.04 with out.04.AccMatch by GridID / Day / Hour
out.04.AccMatch$day <- as.character(out.04.AccMatch$day)

outmatch <- left_join(w.04, out.04.AccMatch, by = c("GRID_ID", "day", "hour"))

# make confusion matrix by roadtype, as nWazeRTX

# At least one freeway 

dx <- outmatch[outmatch$nWazeRT3 > 1 ,]

tn <- sum(dx$TN, na.rm=T) # True Negatives
tp <- sum(dx$TP, na.rm=T) # True Positives
fn <- sum(dx$FN, na.rm=T) # False Negative
fp <- sum(dx$FP, na.rm=T) # False Positives

cat(format(nrow(dx), big.mark = ","), "Observations")

predtab <- as.table(matrix(c(tn, fn, fp, tp), byrow = T, nrow = 2))

bin.mod.diagnostics(predtab)

# At least 1 primary street
dx <- outmatch[outmatch$nWazeRT2 > 1,]

tn <- sum(dx$TN, na.rm=T) # True Negatives
tp <- sum(dx$TP, na.rm=T) # True Positives
fn <- sum(dx$FN, na.rm=T) # False Negative
fp <- sum(dx$FP, na.rm=T) # False Positives

cat(format(nrow(dx), big.mark = ","), "Observations")

predtab <- as.table(matrix(c(tn, fn, fp, tp), byrow = T, nrow = 2))

bin.mod.diagnostics(predtab)


