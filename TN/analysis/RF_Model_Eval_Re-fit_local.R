# This script allows visual evaluation and re-fitting of random forest models, and is designed to be used off SDC with exported model fits and gridded data.

# Setup ----
library(randomForest)
library(tidyverse)

volpedrive = "//vntscex.local/DFS/Projects/PROJ-OS62A1/SDI Waze Phase 2/Output/TN"

grids = c("TN_01dd_fishnet",
          "TN_1sqmile_hexagons")

# <><><><><><>
# Select grid model number, and version (by export date) to evaluate
g = grids[1] # Manually select 1 or 2, or can build a loop.
modelno = "06" # 01 to 06, manually select or loop
version = "2019-02-07"
# <><><><><><>

# Load full input data
w.all <- read.csv(file.path(volpedrive, paste0("Random_Forest_Output_", version), paste0("TN_2017-04_to_2018-03_", g, ".csv")))

# Load fitted model
load(file.path(volpedrive, paste0("Random_Forest_Output_", version), paste0("TN_Model_", modelno, "_", g, "_RandomForest_Output.RData")))

# Visualizations ----

# pdf(file.path(volpedrive, "Figs", paste0("Visualzing_classification_April-Sept_2017_Model_",  modelno, ".pdf")))

out.df <- out.df %>%
  mutate(Obs.bin = Obs > 0)

ggplot(out.df, aes(Prob.Crash, fill = Obs.bin)) + 
  geom_histogram(alpha = 0.5, position = "identity", binwidth = 0.1) +
  ggtitle("Probability of Waze event being categorized as crash")

ggplot(out.df, aes(Prob.Crash, fill = Obs.bin)) + 
  geom_histogram(alpha = 0.5, position = "identity", binwidth = 0.001) + 
  ylim(0, 500) +
  ggtitle("Probability of Waze event being categorized as crash /n (Truncated at count = 500)")

ggplot(out.df, aes(Prob.Crash, fill = Pred)) + 
  geom_histogram(alpha = 0.5, position = "identity", binwidth = 0.005) + 
  ylim(0, 500) +
  ggtitle("Classification Waze event as crash \n (Truncated at count = 500)")

ggplot(out.df, aes(Prob.Crash, fill = Pred)) + 
  geom_histogram(alpha = 0.5, position = "identity", binwidth = 0.01) + 
  ylim(0, 500) +
  facet_wrap(~Obs.bin) +
  ggtitle("Classification Waze event as crash by observed values \n (Truncated at count = 500)")

# dev.off()

### Make aggregation of Waze accidents / minor / major by grid ID
w.grid <- w.all %>%
  group_by(GRID_ID) %>%
  summarise(TotalWazeAccidents = sum(nWazeAccident),
            TotalWazeAccidents_major = sum(nWazeAccidentMajor),
            TotalWazeAccidents_minor = sum(nWazeAccidentMinor),
            TotalTNcrash = sum(nTN_total),
            Urban = max(Waze_UA_U)>1)
  
ggplot(w.grid) + 
  geom_point(aes(TotalTNcrash, TotalWazeAccidents, color = TotalWazeAccidents_major)) +
  facet_wrap(~Urban) +
  geom_abline(slope = 1, intercept = 0)

# TODO 2019-02-07 add model refitting steps. Below is old from first iteration <><><><><><>

omits = c(alwaysomit, alert_subtypes)

fitvars <- names(w.04_09)[is.na(match(names(w.04_09), omits))]

head(out.df)

w.test <- w.04_09[testrows,]

identical(w.test$GRID_ID, out.df$GRID_ID)

# Characteristics of FP with highest prob
w.high.fp <- out.df$FP == TRUE & out.df$Prob.Crash > 0.5

# Characteristcs of FN with lowest prob
w.high.fn <- out.df$FN == TRUE & out.df$Prob.Crash < 0.5

w.test$DayOfWeek <- as.numeric(w.test$DayOfWeek)

#p1 <- prcomp(w.test[fitvars])

all.z <- apply(w.test[fitvars], 2, function(x) all(x==0))

p1 <- prcomp(w.test[fitvars[!all.z]], scale = T)

#p2 <- princomp(w.test[fitvars[!all.z]])

w.group <- data.frame(TN = out.df$TN, TP = out.df$TP, FP.high = w.high.fp, FN.high = w.high.fn)

w.group$TN[w.group$TN==TRUE] = "TN"
w.group$TP[w.group$TP==TRUE] = "TP"
w.group$FP.high[w.group$FP.high==TRUE] = "FP.high"
w.group$FN.high[w.group$FN.high==TRUE] = "FN.high"

w.group$group <- apply(w.group, 1, function(x) x[x!=FALSE][1])

grp  <- as.factor(w.group$group)

prinComp <- data.frame(grp, p1$x)

# from princomp
#prinComp.2 <- data.frame(grp, p2$scores)

library(ggplot2)

pdf("PCA_plot.pdf", width = 8, height = 8)
ggplot(prinComp[sample(1:nrow(prinComp), size = 50000),], aes(x = PC1, y = PC2, color = grp)) + geom_point()
dev.off()

pdf("PCA_plot2.pdf", width = 8, height = 8)

prinComp2 <- prinComp[prinComp$grp == "FP.high" | prinComp$grp == "FN.high",]

prinComp2 <- prinComp2[!is.na(prinComp2$PC1),]

ggplot(prinComp2, aes(x = PC1, y = PC2, color = grp)) + geom_point()

ggplot(prinComp2, aes(x = PC1, y = PC3, color = grp)) + geom_point()

ggplot(prinComp2, aes(x = PC2, y = PC3, color = grp)) + geom_point()

dev.off()

# What are the characteristics that separate these in PCA space?

head(p1$rotation[,1:3])

summary(p1$rotation[,"PC1"])


rownames(p1$rotation)[p1$rotation[,"PC1"] > quantile(p1$rotation[,"PC1"], 0.9)] 
rownames(p1$rotation)[p1$rotation[,"PC1"] < quantile(p1$rotation[,"PC1"], 0.1)] 

rownames(p1$rotation)[p1$rotation[,"PC2"] > quantile(p1$rotation[,"PC2"], 0.9)] 
rownames(p1$rotation)[p1$rotation[,"PC2"] < quantile(p1$rotation[,"PC2"], 0.1)] 

## another take

length(grp)

dim(w.test)

w.test$grp <- grp
f.high <- w.test[w.test$grp == "FN.high" | w.test$grp == "FP.high",]
f.high <- f.high[!is.na(f.high$grp),]

f.high.means <- f.high[c(fitvars, "grp")] %>%
  group_by(grp) %>%
  summarize_all(mean)

write.csv(t(f.high.means), file = "Mean_val_high_FP-FN.csv")

# High false positives: on average 2.5 WazeAccidents, vs. 1 waze accident for high false negatives. High false positives also have more RoadClosed 0.047 vs 0.005 for nigh false negatives.
# High false postives have slightly smaller sum aadt and F system v1

w.test2 <- w.test[!is.na(w.test$grp),]

w.test.means <- w.test2[c(fitvars, "grp")] %>%
  group_by(grp) %>%
  summarize_all(mean)

write.csv(t(w.test.means), file = "Mean_val_W_test.csv")


#### Performance by time of day plot
out.df$day <- as.character(out.df$day)
dd <- data.frame(out.df, w.test) 

d2 <- dd %>% 
  group_by(hour) %>%
  summarize(N = n(),
            TotalWazeAcc = sum(nWazeAccident),
            TotalObserved = sum(nMatchEDT_buffer_Acc),
            TotalEstimated = sum(Pred == "Crash"),
            Obs_Est_diff = TotalObserved - TotalEstimated,
            Pct_Obs_Est = 100 *TotalEstimated / TotalObservedEDT)
  
ggplot(d2, aes(x = hour, y = Pct_Obs_Est)) + geom_line() + ylim(c(50, 120)) +
  ylab("Percent of Observed  crashes Estimated")

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
  ggtitle("Model 30: Estimated crashes / observed /n By hour of day")

