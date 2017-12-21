# This script creates a presentation-ready version of the
# histograms Andy made for EDT and EDT-Waze matches.

# Started by Stephen Brumbaugh @sbrumb

library(tidyverse)
library(lubridate)
library(haven)
library(forcats)
library(scales)
library(extrafont)
library(Cairo)

match <- read_sas('../../SAS/uniqueaccidentsmerge.sas7bdat')
edt <- read_sas('../../SAS/mdcrashfact.sas7bdat')

# Summarize EDT data
summary_edt <- edt %>%
  mutate(hour = HourofDay) %>%
  group_by(WorkWeek, hour) %>%
  summarize(n = n()) %>%
  mutate(freq = n / sum(n),
         source = "All EDT Crashes")

# Summarize EDT data with Waze matches
summary_match <- match %>%
  mutate(hour = hour(CrashTime)) %>%
  group_by(WorkWeek, hour) %>%
  summarize(n = n()) %>%
  mutate(freq = n / sum(n),
         source = "EDT Crashes with Waze Matches")

# Set up the panel plot
summary <- rbind(summary_match, summary_edt) %>%
  mutate(work = factor(WorkWeek),
         work = fct_recode(work, "Weekday" = "Work Week"))

# A E S T H E T I C S
theme_ppt <- theme_void() +
  theme(
    axis.ticks = element_line(color = "gray"),
    text = element_text(family = "Century Gothic", size = 14),
    axis.text = element_text(family = "Century Gothic", size = 10),
    axis.title = element_text(family = "Century Gothic", size = 12,
                              face = "bold"),
    panel.grid.minor = element_blank(),
    panel.grid.major.x = element_blank(),
    panel.grid.major.y = element_line(size = .25, color = "gray"),
    panel.background = element_blank(),
    strip.text = element_text(size = 10, face = "bold"),
    plot.margin = unit(c(.1, .1, .1, .1), "in")
  )

# Plot and save
summary %>%
  ggplot(aes(x = hour)) +
  geom_bar(aes(y = freq, fill = work),
           stat = "identity", width = .8, show.legend = FALSE) +
  scale_y_continuous(labels = percent,
                     breaks = pretty_breaks(n = 5)) +
  labs(x = "Hour of day", y = "Percentage") +
  facet_wrap(~ source + work,
             labeller = labeller(.multi_line = FALSE)) +
  theme_ppt

ggsave("plots/waze reporting.png", type = "cairo-png",
       width = 9, height = 5)
