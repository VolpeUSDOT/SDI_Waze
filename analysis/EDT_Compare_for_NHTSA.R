# EDT Waze Compare for NHTSA
# Use Waze_EDT_2020.zip stored in \\vntscex.local\DFS\Projects\PROJ-OS62A1\SDI Waze Phase 2\Data\EDT, here just calling from local for speed

library(tidyverse)
library(zip)

datadir <- '~/Temp_Working_Docs/SDI/Waze_EDT_2020'

# unzip(zipfile = file.path(datadir, "Waze_EDT_2020.zip"))

states = c('CT', 'MD', 'UT', 'VA')

state = 'MD'

keep_cols = c('GRID_ID', 'time', 'alert_type', 'sub_type', 'road_type',
              'GRID_ID.edt', 'CrashDate', 'DayOfWeek', 'MinuteofDay', 'HourofDay',
              'EDT_UA_Type', 'EDT_UA_Name')

to_load <- dir(datadir)[grep(state, dir(datadir))]

MD_d <- vector()

for(i in 1:length(to_load)){
  
  load(file.path(datadir, to_load[i]))
  
  MD_d <- rbind(MD_d, link.waze.edt[keep_cols])
}

MD_d$CrashDate <- as.Date(as.character(MD_d$CrashDate))

# Count both alert types and EDT crashes by grid ID, date, hour, and EDT_UA_Type

waze_counts <- MD_d %>%
  filter(alert_type == 'ACCIDENT' & !is.na(alert_type)) %>%
  mutate(w_date = format(time, '%Y-%m-%d'),
         w_hr = format(time, '%H')) %>%
  group_by(w_date, w_hr, GRID_ID, EDT_UA_Type) %>%
  summarize(waze_crash = n())

edt_counts <- MD_d %>%
  filter(!is.na(CrashDate)) %>%
  mutate(e_date = format(CrashDate, '%Y-%m-%d'),
         e_hr = formatC(HourofDay, width = 2, flag = 0)) %>%
  group_by(e_date, e_hr, GRID_ID.edt, EDT_UA_Type) %>%
  summarize(edt_crash = n())

# Without doing any matching
# EDT crashes in MD in 2018 = 1,668,620
# Waze crash reports in MD in 2018 = 179,496

d_join <- full_join(waze_counts, edt_counts,
                    by = c('w_date' = 'e_date',
                           'w_hr' = 'e_hr',
                           'GRID_ID' = 'GRID_ID.edt',
                           'EDT_UA_Type')) %>%
  rename(date = w_date,
         hour = w_hr)

d_join <- d_join %>%
  ungroup() %>%
  mutate(urban = !is.na(EDT_UA_Type),
         commuting = hour %in% c('06', '07', '08', '09',
                                 '17','18','19','20'),
         waze_crash =  ifelse(is.na(waze_crash), 0, waze_crash),
         edt_crash =  ifelse(is.na(edt_crash), 0, edt_crash))

d_join$commuting <- factor(d_join$commuting,
                              labels = c('Non Commuting Hours',
                                         'Commuting Hours'))

d_join$urban <- factor(d_join$urban,
                           labels = c('Non Urban',
                                      'Urban'))

# Group to hourly counts by month, for Rural and Urban

d_hrly <- d_join %>%
  ungroup() %>%
  mutate(month = format(as.Date(date), '%m')) %>%
  group_by(month, hour, commuting, urban) %>%
  summarize(waze_crash = sum(waze_crash),
            edt_crash = sum(edt_crash))


# Plot ----

gp1 <- ggplot(d_join, aes(x = waze_crash, y = edt_crash)) +
  geom_point(color = scales::alpha('midnightblue', 0.1)) +
  # geom_abline(intercept = 0, slope = 1, lty = 2) +
  # geom_smooth(method = 'lm') +
  theme_bw() +
  ggtitle('Hourly counts of EDT and Waze crashes per 1 mi grid in MD, 2018')

gp1


gp2 <- ggplot(d_hrly, aes(x = waze_crash, y = edt_crash)) +
  geom_point(color = scales::alpha('midnightblue', 0.8)) +
  # geom_abline(intercept = 0, slope = 1, lty = 2) +
  geom_smooth(method = 'lm') +
  theme_bw() +
  facet_wrap(~commuting + urban) +
  ylab('EDT crash reports') + xlab('Waze crash reports') +
  ggtitle('Counts of EDT and Waze crashes per 1 mi grid in MD, 2018 \n Grouped by hour and month')

gp2

ggsave('Waze_EDT_Compare_MD.jpeg',
       width = 7, height = 6)

# Repeat for VA ----

state = 'VA'

keep_cols = c('GRID_ID', 'time', 'alert_type', 'sub_type', 'road_type',
              'GRID_ID.edt', 'CrashDate', 'DayOfWeek', 'MinuteofDay', 'HourofDay',
              'EDT_UA_Type', 'EDT_UA_Name')

to_load <- dir(datadir)[grep(state, dir(datadir))]

VA_d <- vector()

for(i in 1:length(to_load)){
  
  load(file.path(datadir, to_load[i]))
  
  VA_d <- rbind(VA_d, link.waze.edt[keep_cols])
}

VA_d$CrashDate <- as.Date(as.character(VA_d$CrashDate))

# Count both alert types and EDT crashes by grid ID, date, hour, and EDT_UA_Type

waze_counts <- VA_d %>%
  filter(alert_type == 'ACCIDENT' & !is.na(alert_type)) %>%
  mutate(w_date = format(time, '%Y-%m-%d'),
         w_hr = format(time, '%H')) %>%
  group_by(w_date, w_hr, GRID_ID, EDT_UA_Type) %>%
  summarize(waze_crash = n())

edt_counts <- VA_d %>%
  filter(!is.na(CrashDate)) %>%
  mutate(e_date = format(CrashDate, '%Y-%m-%d'),
         e_hr = formatC(HourofDay, width = 2, flag = 0)) %>%
  group_by(e_date, e_hr, GRID_ID.edt, EDT_UA_Type) %>%
  summarize(edt_crash = n())

# Without doing any matching
# EDT crashes in VA in 2018 = 1,668,620
# Waze crash reports in VA in 2018 = 179,496

d_join <- full_join(waze_counts, edt_counts,
                    by = c('w_date' = 'e_date',
                           'w_hr' = 'e_hr',
                           'GRID_ID' = 'GRID_ID.edt',
                           'EDT_UA_Type')) %>%
  rename(date = w_date,
         hour = w_hr)

d_join <- d_join %>%
  ungroup() %>%
  mutate(urban = !is.na(EDT_UA_Type),
         commuting = hour %in% c('06', '07', '08', '09',
                                 '17','18','19','20'),
         waze_crash =  ifelse(is.na(waze_crash), 0, waze_crash),
         edt_crash =  ifelse(is.na(edt_crash), 0, edt_crash))

d_join$commuting <- factor(d_join$commuting,
                           labels = c('Non Commuting Hours',
                                      'Commuting Hours'))

d_join$urban <- factor(d_join$urban,
                       labels = c('Non Urban',
                                  'Urban'))

# Group to hourly counts by month, for Rural and Urban

d_hrly <- d_join %>%
  ungroup() %>%
  mutate(month = format(as.Date(date), '%m')) %>%
  group_by(month, hour, commuting, urban) %>%
  summarize(waze_crash = sum(waze_crash),
            edt_crash = sum(edt_crash))



d_hrly <- d_join %>%
  ungroup() %>%
  mutate(month = format(as.Date(date), '%m')) %>%
  group_by(month, hour, commuting, urban) %>%
  summarize(waze_crash = sum(waze_crash),
            edt_crash = sum(edt_crash))


# Plot ----

gp1 <- ggplot(d_join, aes(x = waze_crash, y = edt_crash)) +
  geom_point(color = scales::alpha('midnightblue', 0.1)) +
  # geom_abline(intercept = 0, slope = 1, lty = 2) +
  # geom_smooth(method = 'lm') +
  theme_bw() +
  ggtitle('Hourly counts of EDT and Waze crashes per 1 mi grid in VA, 2018')

gp1

ggsave('Waze_EDT_Compare_VA_overall.jpeg',
       width = 7, height = 6)

gp2 <- ggplot(d_hrly, aes(x = waze_crash, y = edt_crash)) +
  geom_point(color = scales::alpha('midnightblue', 0.8)) +
  # geom_abline(intercept = 0, slope = 1, lty = 2) +
  geom_smooth(method = 'lm') +
  theme_bw() +
  facet_wrap(~commuting + urban) +
  ylab('EDT crash reports') + xlab('Waze crash reports') +
  ggtitle('Counts of EDT and Waze crashes per 1 mi grid in VA, 2018 \n Grouped by hour and month')

gp2

ggsave('Waze_EDT_Compare_VA.jpeg',
       width = 7, height = 6)



## NC: total crashes by week for 2019 and 2020 ----

