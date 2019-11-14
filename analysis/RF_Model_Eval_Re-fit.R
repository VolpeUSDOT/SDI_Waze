# This script allows visual evaluation and re-fitting of random forest models

# Setup ----
library(randomForest)
library(tidyverse)

codeloc <- "~/SDI_Waze" 
source(file.path(codeloc, 'utility/get_packages.R')) # installs necessary packages

# Set grid size:
HEXSIZE = 1 

teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

user <- paste0( "/home/", system("whoami", intern = TRUE)) # the user directory to use
localdir <- paste0(user, "/workingdata") # full path for readOGR

outputdir <- file.path(localdir, "Random_Forest_Output")

source(file.path(codeloc, 'utility', 'wazefunctions.R'))

# <><><><><><>
# Loop over states
states = c('CT', 'MD', 'UT', 'VA') 
# <><><><><><>

for(state in states){ # Start state loop ----
  
  # Refit RData model output files ----
  
  # use thse models from multi-state model runs: 18 - 32, inclusive
  modfiles <- dir(outputdir)[grep(paste0(state, '_Model_'), dir(outputdir))]
  modfiles <- modfiles[!grepl('3[3-9]', modfiles)] # Exclude 33 and higher
  # modfiles <- modfiles[grep('^TN_Model_', modfiles)]
  
  counts = vector() # To store confusion matrix outputs
  
  for(i in modfiles){ # i = modfiles[5]
    
    cat(i, "\n")
    
    load(file.path(outputdir, i)) # loads out.df, along with rf.out randomForest object
    
    reference.vec = out.df$Obs
    
    reference.vec <- ifelse(reference.vec == 0, 'NoCrash', 'Crash')
    reference.vec <- as.factor(as.character(reference.vec)) 
    
    
    # look at cutoffs  
    co = c(seq(0.005, 0.01, by = 0.001),
           c(0.02, 0.03, 0.04),
           seq(0.05, 0.75, by = 0.05))
    
    pt.vec = vector()
    
    for(coi in co){ # coi = co[1]
      predx = ifelse(out.df$Prob.Crash >= coi, 'Crash', 'NoCrash')
      predx <- as.factor(as.character(predx)) # Crash is first
      
      predtab <- table(predx, reference.vec, 
                       dnn = c("Predicted","Observed")) 
      if(sum(dim(predtab))==4 ) {bin.diag = bin.mod.diagnostics(predtab)} else {bin.diag = NA}
      
      pt.vec <- cbind(pt.vec, bin.diag)
      cat(coi, ". ")
      
    }
    
    pt.vec = as.data.frame(pt.vec); colnames(pt.vec) = co
    prec.recall <- pt.vec %>%
      gather()
    
    prec.recall$Metric = rep(c(1, 3, 2, 4), ncol(pt.vec))
    prec.recall$Metric <- factor(prec.recall$Metric, labels = c("Accuracy", "Recall","Precision", "False Positive Rate"))
    names(prec.recall)[1:2] = c("Cutoff", "Value")
    
    gp4 <- ggplot(prec.recall, aes(x = Cutoff, y = Value, group = Metric)) + 
      geom_line(aes(color = Metric), size = 2) +
      ggtitle(paste0("Crash classification tradeoffs \n", i)) + 
      theme_bw() +
      annotate("text",
               x = 0.5,
               y = c(0.95, 0.05, 0.46,0.81),
               hjust = 0,
               label = c("Accuracy", "False Positive Rate", "Precision", "Recall"))
    print(gp4); ggsave(filename = file.path(localdir, 'Figures', paste0(i, '_Prec_recall_tradoff.jpg')),
                       width = 8, height = 7)      
    
    recall_prec_diff = pt.vec['recall',]-pt.vec['precision',]
    
    best_cutoff = as.numeric(names(recall_prec_diff[which(abs(recall_prec_diff)==min(abs(recall_prec_diff)))][1]))
    # After setting cutoffs, run this:
    
    out.df$Pred = ifelse(out.df$Prob.Crash >= best_cutoff, 'Crash', 'NoCrash')
     
    names(out.df)[4:8] <- c("hour", "Obs", "Pred", "Prob.Noncrash", "Prob.Crash")
    out.df = data.frame(out.df[1:8],
                        TN = out.df$Obs == 0 &  out.df$Pred == "NoCrash",
                        FP = out.df$Obs == 0 &  out.df$Pred == "Crash",
                        FN = out.df$Obs == 1 &  out.df$Pred == "NoCrash",
                        TP = out.df$Obs == 1 &  out.df$Pred == "Crash"
                        )
  
    w.group <- data.frame(TN = out.df$TN, TP = out.df$TP, FP = out.df$FP, FN = out.df$FN)
    
    w.group$TN[w.group$TN==TRUE] = "TN"
    w.group$TP[w.group$TP==TRUE] = "TP"
    w.group$FP[w.group$FP==TRUE] = "FP"
    w.group$FN[w.group$FN==TRUE] = "FN"
    
    w.group$group <- apply(w.group, 1, function(x) x[x!=FALSE][1])
    
    out.df$Pred.grp <- as.factor(w.group$group)
    
    co = out.df %>%
      summarize(TN = sum(TN),
                FP = sum(FP),
                FN = sum(FN),
                TP = sum(TP))
  
    predtab <- table(predx, reference.vec, 
                     dnn = c("Predicted","Observed")) 
    if(sum(dim(predtab))==4 ) {bin.diag = bin.mod.diagnostics(predtab)} else {bin.diag = NA}
    
    # plot(pROC::roc(out.df$Obs, out.df$Prob.Crash, auc = TRUE))
    
    (model_auc <- pROC::auc(out.df$Obs, out.df$Prob.Crash))
    
    modelno = strsplit(i, "_")
    modelno = modelno[[1]][grep('\\d{2}', modelno[[1]])] 
    
    co = data.frame(modelno, co, t(bin.diag), AUC = as.numeric(model_auc))
    
    counts = rbind(counts, co)
    system(paste('mkdir -p', file.path(outputdir, 'Refit')))
    write.csv(out.df, file = file.path(outputdir, 'Refit', i), row.names=F)
  
    # Variable importance ----
    
    pdf(file.path(localdir, "Figures", paste0(state, "_Variable_Importance_Model_", modelno, ".pdf")), width = 8, height = 8)
    
    varImpPlot(rf.out, 
               main = paste(state, "Model",modelno,"Variable Importance"),
               bg = scales::alpha("midnightblue", 0.5),
               n.var = 15)
    
    dev.off()

  } # End model loop
  write.csv(counts, file = file.path(outputdir, 'Refit', paste0("Confusion_matrix_counts_", state, ".csv")))
} # End state loop

# Export results ----

# Send re-fit model outputs, all inputs, and models themselves to the export_request directory

# Get multistate output log files and all inputs
out1 <- dir()[grep('^\\w{2}_', dir())]
out1 <- file.path(localdir, out1[!grepl('TN', out1)])

# Get refit ouputs
out_refit <- file.path(localdir, 'Random_Forest_Output', 'Refit', dir(file.path(localdir, 'Random_Forest_Output', 'Refit')))

# Get models
out_mod <- dir(file.path(localdir, 'Random_Forest_Output'))[grep('^\\w{2}_', dir(file.path(localdir, 'Random_Forest_Output')))]
out_mod <- file.path(localdir, 'Random_Forest_Output', out_mod)

filestozip = c(out1, out_refit, out_mod)

zipname = paste0('Multi-State_All_Output_', Sys.Date(), '.zip')

system(paste('zip ', file.path('~/workingdata', zipname),
             paste(filestozip, collapse = " ")))

system(paste(
  'aws s3 cp',
  file.path('~/workingdata', zipname),
  file.path(teambucket, 'export_requests', zipname)
))


# Separate outputs to make smaller zips
zipname = paste0('Multi-State_Model_Output_1_', Sys.Date(), '.zip')

system(paste('zip ', file.path('~/workingdata', zipname),
             paste(out1, collapse = " ")))

system(paste(
  'aws s3 cp',
  file.path('~/workingdata', zipname),
  file.path(teambucket, 'export_requests', zipname)
))

zipname = paste0('Multi-State_Model_Output_2_', Sys.Date(), '.zip')

system(paste('zip ', file.path('~/workingdata', zipname),
             paste(out_refit, collapse = " ")))

system(paste(
  'aws s3 cp',
  file.path('~/workingdata', zipname),
  file.path(teambucket, 'export_requests', zipname)
))

zipname = paste0('Multi-State_Model_Output_3_', Sys.Date(), '.zip')

system(paste('zip ', file.path('~/workingdata', zipname),
             paste(out_mod, collapse = " ")))

system(paste(
  'aws s3 cp',
  file.path('~/workingdata', zipname),
  file.path(teambucket, 'export_requests', zipname)
))


# Additional visualizations ----
# The following is a collection of additional visualizations to plot vairable importance, outcomes of the classification model, performance by time of day, and other plots. Not run by default, because these were written for specific models in mind, or haven't been otherwise made flexible to accomodate any model.

RUNADDITIONAL = F

if(RUNADDITIONAL) {
# Classification ----
pdf(file.path(localdir, "Figures", paste0(state, "_Visualzing_classification_Model_", modelno, ".pdf")), width = 10, height = 10)

out.df$CorrectPred = out.df$TN | out.df$TP
levels(out.df$Obs) = c("Obs = NoCrash", "Obs = Crash")

out.df <- out.df %>%
  mutate(Obs.bin = 1*Obs > 0)

gp1 <- ggplot(out.df, aes(Prob.Crash, fill = Obs)) + 
  geom_histogram(alpha = 0.5, position = "identity", binwidth = 0.01) + 
  scale_y_continuous(limits=c(0, 5000), oob = scales::rescale_none) + 
  ggtitle("Probability of Waze event being categorized as EDT crash \n (Truncated at count = 5000)") + 
  geom_vline(xintercept = 0.225, linetype = 'dotted') +
  annotate("text", x = 0.325, y = 5000, label = "Cutoff = 0.225") +
  xlab("Estimated Crash Probability") +
  #theme_bw() + 
  scale_fill_brewer(palette="Set1")

print(gp1)

gp2 <- ggplot(out.df, aes(Prob.Crash, fill = CorrectPred)) + 
  geom_histogram(alpha = 0.5, position = "identity", binwidth = 0.01) + 
  scale_y_continuous(limits=c(0, 5000), oob = scales::rescale_none) + 
  geom_vline(xintercept = 0.225, linetype = 'dotted') +
  facet_wrap(~Obs.bin) +
  xlab("Estimated Crash Probability") +
  scale_fill_brewer(palette="Set1") +
  ggtitle("Frequency of classification as crash by observed values \n (Truncated at count = 500; Max = 600,000)")

print(gp2)
# Plotting historgram of difference from observed and estimated, by grid cell aggregated over time ----
levels(out.df$Pred) = c(1, 0) # from Crash, NoCrash
levels(out.df$Obs.bin) = c(0, 1) # from Obs  =NoCrash, Obs = Crash

pct.diff.grid <- out.df %>%
  mutate(nObs = as.numeric(as.character(Obs.bin)),
         nPred = as.numeric(as.character(Pred))) %>%
  group_by(GRID_ID) %>%
  summarize(sumObs = sum(nObs),
            sumPred = sum(nPred),
            Pct.diff = 100*(sumPred - sumObs) / sumObs)

pct.diff.grid$Pct.diff[is.na(pct.diff.grid$Pct.diff) | pct.diff.grid$Pct.diff == Inf] = 0

#hist(pct.diff.grid$Pct.diff)

# Aggregate for report table
pct.diff.table = as.data.frame(table(pct.diff.cut <- cut(pct.diff.grid$Pct.diff, breaks = c(-2000, -100, -50, -1, 0, 50, 100, 2000))))
# https://drsimonj.svbtle.com/pretty-histograms-with-ggplot2. Trick is fill = cut()

# levels(cut(pct.diff.grid$Pct.diff, 25)) # create manual colors to match tablesu 

gp3 <- ggplot(pct.diff.grid, aes(Pct.diff, fill = cut(Pct.diff, 
                                                      breaks = c(-1000, -100, -50, -25, 
                                                                 0, 
                                                                 25, 50, 100, 1000) ))) + 
  geom_histogram(bins = 10, show.legend = F, binwidth = 10) +
  theme_dark() +
  ggtitle("Summary of percent difference from observed and estimated EDT-level crashes") +
  xlab("Percent difference") + ylab("Frequency by GRID_ID") +
  scale_fill_brewer(palette = "RdBu", direction = -1)
#
#  scale_color_gradient(low = 'red', high = 'midnightblue')

print(gp3)

# TODO 2019-02-07 add model refitting steps. Below is old from first iteration <><><><><><>

# RF inputs
response.var = "MatchEDT_buffer_Acc"
alwaysomit = c(grep("GRID_ID", names(w.allmonths), value = T), "day", "hextime", "year", "weekday", "vmt_time",
               "uniqWazeEvents", "nWazeRowsInMatch", 
               "nMatchWaze_buffer", "nNoMatchWaze_buffer",
               grep("EDT", names(w.allmonths), value = T))

alert_types = c("nWazeAccident", "nWazeJam", "nWazeRoadClosed", "nWazeWeatherOrHazard")

alert_subtypes = c("nHazardOnRoad", "nHazardOnShoulder" ,"nHazardWeather", "nWazeAccidentMajor", "nWazeAccidentMinor", "nWazeHazardCarStoppedRoad", "nWazeHazardCarStoppedShoulder", "nWazeHazardConstruction", "nWazeHazardObjectOnRoad", "nWazeHazardPotholeOnRoad", "nWazeHazardRoadKillOnRoad", "nWazeJamModerate", "nWazeJamHeavy" ,"nWazeJamStandStill",  "nWazeWeatherFlood", "nWazeWeatherFog", "nWazeHazardIceRoad")


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


#### Performance by time of day plot ----
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

}
