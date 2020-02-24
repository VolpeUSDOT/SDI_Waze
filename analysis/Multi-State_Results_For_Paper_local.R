# 4 state model eval
library(tidyverse)
library(readr)

wd <- '//vntscex.local/DFS/Projects/PROJ-OS62A1/SDI Waze Phase 2/Output/Random_Forest_Output/Multi-state_2019/Refit'

setwd(wd)

states = c('CT', 'MD', 'VA', 'UT')

mods = c(18, 24, 30, 31) 

mod_combos <- expand.grid(states, mods)

mods <- paste0(mod_combos[,1], '_Output_Model_', mod_combos[,2],'.csv')

# Loop over model outputs, make hourly summaries

hourly <- list()
commuting <- list()


for(m in mods){ # m = mods[1]
  mo <- read_csv(m)
  
  d2 <- mo %>% select(-Hour) %>% 
    group_by(hour) %>%
    summarize(N = n(),
              TotalObservedEDT = sum(Obs),
              TotalEstimated = sum(Pred == 'Crash'),
              Obs_Est_diff = TotalObservedEDT - TotalEstimated,
              Pct_Obs_Est = 100 *TotalEstimated / TotalObservedEDT)
  
  hourly[[m]] <- d2
  
  
  d3 <- mo %>% select(-Hour) %>% 
    mutate(commuting = ifelse(hour %in% c(7, 8, 9, 17, 18, 19), TRUE, FALSE)) %>%
    group_by(commuting) %>%
    summarize(N = n(),
              TotalObservedEDT = sum(Obs),
              TotalEstimated = sum(Pred == 'Crash'),
              Obs_Est_diff = TotalObservedEDT - TotalEstimated,
              Pct_Obs_Est = 100 *TotalEstimated / TotalObservedEDT)
  
  commuting[[m]] <- d3
  
  ggplot(d2, aes(x = hour, y = Pct_Obs_Est)) + geom_line() + #ylim(c(90, 120)) +
    ylab("Percent of Observed EDT crashes Estimated") + 
    ggtitle(m)
  
  cat(rep("<<>>", 10), '\n', m, '\n')
  
  print(as.data.frame(d3))
  
  cat('\n\n')
 
}

# Output is two lists, hourly and commuting. Each one has a summary by state and model
hourly

hourly_df <- vector()
commuting_df <- vector()

for(l in 1:length(hourly)){
  hl <- hourly[[l]]
  hl$state_mod <- names(hourly)[l]
  hourly_df <- rbind(hourly_df, hl)
  }

hourly_df = hourly_df %>% 
  mutate(state = substr(state_mod, 1, 2),
         model = substr(state_mod, 17, 18),
         early_am = hour > 1 & hour < 7,
         commuting = (hour %in% c(7, 8, 9, 17, 18, 19)))

hourly_df %>%
  filter(early_am == TRUE) %>%
  group_by(state, model) %>%
  summarize(100 - mean(Pct_Obs_Est))

# Average underestimation in early morning hours for the best model (30), across the four states.
hourly_df %>%
  filter(early_am == TRUE, model == 30) %>%
  group_by(state) %>%
  summarize(100 - mean(Pct_Obs_Est))

# Average overestimation in commuting hours for the best model (30), across the four states.
hourly_df %>%
  filter(commuting == TRUE, model == 30) %>%
    group_by(state) %>%
  summarize(100 - mean(Pct_Obs_Est))


hourly_df %>%
  filter(model == 30) %>%
  group_by(state) %>%
  summarize(mean(Pct_Obs_Est))


# Variable importance ----
# Focus on model 30

mod_combos = mod_combos[mod_combos$Var2 == 30,]

mods <- paste0(mod_combos[,1], '_Model_', mod_combos[,2],'_RandomForest_Output.RData')

for(m in mods){ # m = mods[1]
  mo <- load(m)
  
  d2 <- mo %>% select(-Hour) %>% 
    group_by(hour) %>%
    summarize(N = n(),
              TotalObservedEDT = sum(Obs),
              TotalEstimated = sum(Pred == 'Crash'),
              Obs_Est_diff = TotalObservedEDT - TotalEstimated,
              Pct_Obs_Est = 100 *TotalEstimated / TotalObservedEDT)
  
  hourly[[m]] <- d2
  
  
  d3 <- mo %>% select(-Hour) %>% 
    mutate(commuting = ifelse(hour %in% c(7, 8, 9, 17, 18, 19), TRUE, FALSE)) %>%
    group_by(commuting) %>%
    summarize(N = n(),
              TotalObservedEDT = sum(Obs),
              TotalEstimated = sum(Pred == 'Crash'),
              Obs_Est_diff = TotalObservedEDT - TotalEstimated,
              Pct_Obs_Est = 100 *TotalEstimated / TotalObservedEDT)
  
  commuting[[m]] <- d3
  
  ggplot(d2, aes(x = hour, y = Pct_Obs_Est)) + geom_line() + #ylim(c(90, 120)) +
    ylab("Percent of Observed EDT crashes Estimated") + 
    ggtitle(m)
  
  cat(rep("<<>>", 10), '\n', m, '\n')
  
  print(as.data.frame(d3))
  
  cat('\n\n')
  
}