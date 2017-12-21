# This script creates a presentation-ready version of the
# histograms Andy made for EDT and EDT-Waze matches.

# Started by Stephen Brumbaugh @sbrumb

library(extrafont)
extrafont::loadfonts(device="win") # needed on Windows machines
# extrafont::font_import() # will import all fonts, takes time

library(tidyverse)
library(lubridate)
library(haven)
library(forcats)
library(scales)
library(Cairo)
library(gridExtra) # for grid.arrange

# Code location
mappeddriveloc <- "W:"

codeloc <- "~/git/SDI_Waze"
wazedir <- file.path(mappeddriveloc, "SDI Pilot Projects/Waze/Working Documents")

setwd(wazedir)

match <- read_sas('SAS/uniqueaccidentsmerge.sas7bdat')
edt <- read_sas('SAS/mdcrashfact.sas7bdat')

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
p1 <- summary %>%
  ggplot(aes(x = hour)) +
  geom_bar(aes(y = freq, fill = work),
           stat = "identity", width = .8, show.legend = FALSE) +
  scale_y_continuous(labels = percent,
                     breaks = pretty_breaks(n = 5)) +
  labs(x = "Hour of day", y = "Percentage") +
  facet_wrap(~ source + work,
             labeller = labeller(.multi_line = FALSE)) +
  theme_ppt

p1

ggsave("R/Graphics/plots/waze reporting.png", type = "cairo-png",
       width = 9, height = 5)


## Add a difference panel

summary_difference <- summary_match
summary_difference$freq <- summary_match$freq - summary_edt$freq
summary_difference$source <- "zDifference"


summary2 <- rbind(summary_match, summary_edt, summary_difference) %>%
  mutate(work = factor(WorkWeek),
         work = fct_recode(work, "Weekday" = "Work Week"))

# Plot and save
p2 <- summary2 %>%
  ggplot(aes(x = hour)) +
  geom_bar(aes(y = freq, fill = work),
           stat = "identity", width = .8, show.legend = FALSE) +
  scale_y_continuous(labels = percent,
                     breaks = pretty_breaks(n = 5)) +
  labs(x = "Hour of day", y = "Percentage") +
  facet_wrap(~source + work,
             nrow = 3, ncol = 2,
             # scales = 'free',
             labeller = labeller(.multi_line = FALSE)) +
  theme_ppt

p2 

# Another idea: plot separately and use grid.arrange, make postitive and negative values have different colors, set y-axis limits for the differene plot smaller.

summary_difference <- summary_match
summary_difference$freq <- summary_match$freq - summary_edt$freq
summary_difference$source <- "Difference"
summary_difference$sign <- ifelse(summary_difference$freq >= 0, 'positive', 'negative')

summary_difference <- summary_difference %>%
  mutate(work = factor(WorkWeek),
         work = fct_recode(work, "Weekday" = "Work Week"))

p3 <- summary_difference %>%
  ggplot(aes(x = hour)) +
  geom_bar(aes(y = freq, fill = sign),
           stat = "identity", width = .8, show.legend = FALSE) +
  facet_wrap(~source + work,
             labeller = labeller(.multi_line = FALSE)) +
  scale_fill_manual(values = c("positive" = "midnightblue", "negative" = "firebrick3")) +
  scale_y_continuous(labels = percent,
                    breaks = pretty_breaks(n = 3)) +
  labs(x = "Hour of day", y = "") +
  theme_ppt
p3


# grid.arragne to show, arrangeGrob to make new plot object
grid.arrange(p1, p3,
             nrow = 2,
             heights = c(2.5, 1))

p4 <- arrangeGrob(p1, p3,
            nrow = 2,
            heights = c(2.5, 1))

ggsave("R/Graphics/plots/waze_reporting_difference.png", 
       plot = p4,
       type = "cairo-png",
       width = 9, height = 5)
system("open R/Graphics/plots/waze_reporting_difference.png")
