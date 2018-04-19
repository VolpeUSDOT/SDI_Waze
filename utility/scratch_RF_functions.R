# turning random forest model chunks into a function

run.RF

trainrows <- sort(sample(1:nrow(w.04), size = nrow(w.04)*.7, replace = F))
testrows <- (1:nrow(w.04))[!1:nrow(w.04) %in% trainrows]

system.time(rf.04 <- foreach(ntree = rep(ntree.use/avail.cores, avail.cores),
                             .combine = combine, .multicombine=TRUE,
                             .packages = "randomForest") %dopar% {
                               randomForest(wazeAccformula, data = w.04[trainrows,], ntree = ntree) })

rf.04.pred <- predict(rf.04, w.04[testrows, fitvars])

Nobs <- data.frame(t(c(nrow(w.04),
                       summary(w.04$MatchEDT_buffer),
                       length(w.04$nWazeAccident[w.04$nWazeAccident>0]) )))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
(predtab <- table(w.04$MatchEDT_buffer[testrows], rf.04.pred)) 

keyoutputs[[modelno]] = list(Nobs,
                             predtab,
                             diag = bin.mod.diagnostics(predtab)
)

# save output predictions
out.04 <- data.frame(w.04[testrows, c("GRID_ID", "day", "hour", "MatchEDT_buffer")], rf.04.pred)
out.04$day <- as.numeric(out.04$day)
names(out.04)[4:5] <- c("Obs", "Pred")
out.04 = data.frame(out.04,
                    TN = out.04$Obs == 0 &  out.04$Pred == 0,
                    FP = out.04$Obs == 0 &  out.04$Pred == 1,
                    FN = out.04$Obs == 1 &  out.04$Pred == 0,
                    TP = out.04$Obs == 1 &  out.04$Pred == 1)
write.csv(out.04,
          file = paste(modelno, "RandomForest_pred_04.csv", sep = "_"),
          row.names = F)
s3save(list = c("rf.04",
                "rf.04.pred",
                "testrows",
                "trainrows",
                "w.04",
                "out.04"),
       object = file.path(outputdir, paste("Model", modelno, "RandomForest_Output_04.RData", sep= "_")),
       bucket = waze.bucket)