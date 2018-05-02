# scratch corr plots
# after running random forest full
library(ggplot2)
library(ggcorrplot) # install.packages("ggcorrplot",dep=T)
library(plotly) # install.packages("plotly",dep=T)

names(w.04_09)


respvars <- c("nMatchEDT_buffer_Acc")#, "MatchEDT_buffer_Acc")
wazevars1 <- c(alert_types, alert_subtypes)
wazevars2 <- c(
              grep("MagVar", names(w.04), value = T), # direction of travel
              grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
              grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
              grep("nWazeJam_", names(w.04), value = T) )
suppvars <- c("wx",
         c("CRASH_SUM", "FATALS_SUM"), # FARS variables,
         grep("F_SYSTEM", names(w.04), value = T), # road class
         c("MEAN_AADT", "SUM_AADT", "SUM_miles"), # AADT
         grep("WAC", names(w.04), value = T), # Jobs workplace
         grep("RAC", names(w.04), value = T) # Jobs r
)


pdf("Correlations.pdf", width = 8, height = 8)

cormat <- round(cor(w.04[c(respvars, wazevars1)]), 4)
p.mat <- cor_pmat(w.04[c(respvars, wazevars1)])


gp <- ggcorrplot(cormat, method = "circle", type = "upper",
           p.mat = p.mat,
           insig = "pch",
           pch = 16,
           pch.col = alpha("grey80", .8)
           )
gp + ggtitle("Correlations between Waze event types+subtypes and EDT crashes")

cormat <- round(cor(w.04[c(respvars, alert_types)]), 4)
p.mat <- cor_pmat(w.04[c(respvars, alert_types)])
gp <- ggcorrplot(cormat, method = "circle", type = "upper",
                 p.mat = p.mat,
                 insig = "pch",
                 pch = 16,
                 pch.col = alpha("grey80", .8)
)
gp + ggtitle("Correlations between Waze event types and EDT crashes")


cormat <- round(cor(w.04[c(respvars, alert_subtypes)]), 4)
p.mat <- cor_pmat(w.04[c(respvars, alert_subtypes)])
gp <- ggcorrplot(cormat, method = "circle", type = "upper",
                 p.mat = p.mat,
                 insig = "pch",
                 pch = 16,
                 pch.col = alpha("grey80", .8)
)
gp + ggtitle("Correlations between Waze event subtypes and EDT crashes")



# ggplotly(gp)

cormat <- round(cor(w.04[c(respvars, wazevars2)]), 4)
p.mat <- cor_pmat(w.04[c(respvars, wazevars2)])

gp <- ggcorrplot(cormat, method = "circle", type = "upper",
                 p.mat = p.mat,
                 insig = "pch",
                 pch = 16,
                 pch.col = alpha("grey80", .8)
)
gp + ggtitle("Correlations between Waze supplmental variables and EDT crashes")



cormat <- round(cor(w.04[c(respvars, suppvars)]), 4)
p.mat <- cor_pmat(w.04[c(respvars, suppvars)])

gp <- ggcorrplot(cormat, method = "circle", type = "upper",
                 p.mat = p.mat,
                 insig = "pch",
                 pch = 16,
                 pch.col = alpha("grey80", .8)
)
gp + ggtitle("Correlations between Additional variables and EDT crashes")
dev.off()
