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