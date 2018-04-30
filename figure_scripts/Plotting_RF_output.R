# Exploring fitted values for plotting

# Load full input for model 23

ostdrive = "W:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/HexagonWazeEDT/WazeEDT Agg1mile Rdata Input"
volpedrive = "//vntscex.local/DFS/Projects/PROJ-OR02A2/SDI/Model_Output"

load(file.path(ostdrive, "WazeEDT_Agg1mile_04-09.RData"))
load(file.path(volpedrive, "RF2", "Model_23_RandomForest_Output.RData"))

out.df$Correct = out.df$TN | out.df$TP

library(ggplot2)
pdf("Visualzing_classification_April-Sept_2017_Model_23.pdf")
#plot(out.df$Prob.Crash ~ out.df$Obs)
# ggplot(out.df) + geom_violin(aes(Obs, Prob.Crash), scale = "area")
ggplot(out.df, aes(Prob.Crash, fill = Obs)) + 
  geom_histogram(alpha = 0.5, position = "identity", binwidth = 0.1) +
  ggtitle("Probability of Waze event being categorized as EDT crash")

ggplot(out.df, aes(Prob.Crash, fill = Obs)) + 
  geom_histogram(alpha = 0.5, position = "identity", binwidth = 0.001) + 
  ylim(0, 500) +
  ggtitle("Probability of Waze event being categorized as EDT crash \n (Truncated at count = 500)")

ggplot(out.df, aes(Prob.Crash, fill = Pred)) + 
  geom_histogram(alpha = 0.5, position = "identity", binwidth = 0.005) + 
  ylim(0, 500) +
  ggtitle("Classification Waze event as EDT crash \n (Truncated at count = 500)")

ggplot(out.df, aes(Prob.Crash, fill = Pred)) + 
  geom_histogram(alpha = 0.5, position = "identity", binwidth = 0.01) + 
  ylim(0, 500) +
  facet_wrap(~Obs) +
  ggtitle("Classification Waze event as EDT crash by observed values \n (Truncated at count = 500)")

ggplot(out.df, aes(Prob.Crash, fill = Correct)) + 
  ylim(0, 500) +
  geom_histogram(alpha = 0.5, position = "dodge", binwidth = 0.001) + 
  facet_wrap(~Pred) +
  ggtitle("Classification Waze event as EDT crash by predicted values \n (Truncated at count = 500)")

ggplot(out.df, aes(Prob.Crash, fill = Correct)) + 
  geom_freqpoly(alpha = 0.5, position = "identity", binwidth = 0.01) + 
  ylim(0, 500) +
  facet_wrap(~Pred) +
  ggtitle("Classification Waze event as EDT crash by predicted values \n (Truncated at count = 500)")


dev.off()


### Make aggregation of Waze accidents / minor / major by grid ID, April - Sept

w.grid <- w.04_09 %>%
  group_by(GRID_ID) %>%
  summarise(TotalWazeAccidents = sum(nWazeAccident),
            TotalWazeAccidents_major = sum(nWazeAccidentMajor),
            TotalWazeAccidents_minor = sum(nWazeAccidentMinor),
            TotalEDTcrash = sum(nEDTMaxDamDisabling + nEDTMaxDamFunctional + nEDTMaxDamMinor + nEDTMaxDamNone + nEDTMaxDamNotReported + nEDTMaxDamUnknown)
  )

write.csv(w.grid, file.path(volpedrive, "WazeEDT_Grid_counts_04-09.csv"), row.names = F)
