# Plotting script for special events -- build off of Hot_spot_1_Event.R


pdf(file.path("Figures", "Hot_Spot_data_prep.pdf"), width = 8, height = 8)
plot(zoom_box, main = 'Zooming to FedEx field, September 2017', sub='3 mile buffer')
plot(ws, add =T, col = scales::alpha("grey20", 0.2))
plot(es, add =T, col = scales::alpha("firebrick", 0.9))
legend('topleft', pch = "+", col = c('black', 'red'), legend = c('Waze events', 'EDT crashes'))
dev.off()