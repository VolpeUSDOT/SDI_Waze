# Random Forest functions for Waze
require(pROC)
######################################################################
# do.rf function for running random forest models in parallel ----

# Arguments:
# train.dat - Data frame containing all the predictors and the response variable

# omits - Vector of column names in the data to omit from the model. Used to generate the model formula, if the formula is not otherwise provided in the `formula` argument.

# response.var - Vector of the data to use as the response variable, e.g. MatchEDT_buffer_Acc or nMatchEDT_buffer_Acc

# model.no - character value to keep track of model number used; cannot be left blank

# formula - If provided, use this formula instead of the automatically generated one for the random forest model

# test.dat - If provided, this will be used to test the random forest model. If not provided, a split of the training data will be used according to the test.split argument. Both test.dat and test.split cannot be provided.

# test.split - if test.dat is not specified, use this to randomly split the training data into two portions by row, with this split value being used as the test proportion of the data.

# rf.inputs - list of arguments to pass to randomForest

# thin.dat - value from 0 to 1 for proportion of the training and test data to use in fitting the model. E.g. thin.dat = 0.2, use only 20% of the training and test data; useful for testing new features. 

do.rf <- function(train.dat, omits, response.var = "MatchEDT_buffer_Acc", model.no,
                  # rf.formula = NULL, 
                  test.dat = NULL, test.split = .30,
                  thin.dat = NULL,
                  rf.inputs = list(ntree.use = 500, avail.cores = 4, mtry = NULL, maxnodes = NULL, nodesize = 5)){
  
  if(!is.null(test.dat) & !missing(test.split)) stop("Specify either test.dat or test.split, but not both")

  class(train.dat) <- "data.frame"
  
  # Can delete, not using formula interface any more
  # # If no formula provided, create one from names of training data - vector of omits. 
  # # If formula provided, extract the RHS of formula as the fitvars vector of predictor names
  # if(is.null(rf.formula)){
  #     fitvars <- names(train.dat)[is.na(match(names(train.dat), omits))]
  #     rf.formula <- reformulate(termlabels = fitvars[is.na(match(fitvars, response.var))], response = response.var)
  #   } else { 
  #     fitvars = attr(terms.formula(rf.formula), "term.labels")
  #   }

  fitvars <- names(train.dat)[is.na(match(names(train.dat), omits))]
  
  # Provide mtry if null
  if(is.null(rf.inputs$mtry)){
    mtry.use = if (!is.factor(response.var)) max(floor(length(fitvars)/3), 1) else floor(sqrt(length(fitvars)))
  } else {mtry.use = rf.inputs$mtry}
  
  # Thin data sets if thin.dat provided
  if(!is.null(thin.dat)) {
    train.dat <- train.dat[sample(1:nrow(train.dat), size = nrow(train.dat)*thin.dat),]
    if(!is.null(test.dat)){
      test.dat <- test.dat[sample(1:nrow(test.dat), size = nrow(test.dat)*thin.dat),]
    }
  }
  
  # 70:30 split or Separate training and test data
  if(is.null(test.dat)){
    trainrows <- sort(sample(1:nrow(train.dat), size = nrow(train.dat)*(1-test.split), replace = F))
    testrows <- (1:nrow(train.dat))[!1:nrow(train.dat) %in% trainrows]
     
    rundat = train.dat[trainrows,]
    test.dat.use = train.dat[testrows,]
  } else {
    class(test.dat) <- "data.frame"
    rundat = train.dat
    test.dat.use = test.dat
    comb.dat <- rbind(train.dat, test.dat)
  }
   
 
  # Start RF in parallel
  starttime = Sys.time()
  
  # make a cluster of all available cores
  cl <- makeCluster(parallel::detectCores(), useXDR = F) 
  registerDoParallel(cl)
  
  rf.out <- foreach(ntree = rep(rf.inputs$ntree.use/rf.inputs$avail.cores, rf.inputs$avail.cores),
                  .combine = randomForest::combine, .multicombine=T, .packages = 'randomForest') %dopar% 
    randomForest(x = rundat[,fitvars], y = rundat[,response.var], 
                 ntree = ntree, mtry = mtry.use, 
                 maxnodes = rf.inputs$maxnodes, nodesize = rf.inputs$nodesize,
                 keep.forest = T)
  
  stopCluster(cl); rm(cl); gc(verbose = F) # Stop the cluster immediately after finished the RF
  
  timediff = Sys.time() - starttime
  cat(round(timediff,2), attr(timediff, "unit"), "to fit model", model.no, "\n")
  # End RF in parallel
  
  rf.pred <- predict(rf.out, test.dat.use[fitvars])
  rf.prob <- predict(rf.out, test.dat.use[fitvars], type = "prob")

  Nobs <- data.frame(nrow(rundat),
                 sum(as.numeric(as.character(rundat[,response.var])) == 0),
                 sum(as.numeric(as.character(rundat[,response.var])) > 0),
                 length(rundat$nWazeAccident[train.dat$nWazeAccident>0]) )
  
  colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
  predtab <- table(test.dat.use[,response.var], rf.pred)
  
  # pROC::roc - response, predictor
  model_auc <- pROC::auc(test.dat.use[,response.var], rf.prob[,colnames(rf.prob)=="1"])

  plot(pROC::roc(test.dat.use[,response.var], rf.prob[,colnames(rf.prob)=="1"]),
      main = paste0("Model ", model.no),
      grid=c(0.1, 0.2))
  legend("bottomright", legend = round(model_auc, 4), title = "AUC", inset = 0.1)

  dev.print(device = jpeg, file = paste0("AUC_", model.no, ".jpg"), width = 500, height = 500)

  # AUC::roc - predictions, labels
  # plot(model_auc2 <- AUC::roc(rf.out$votes[,"1"], factor(1*(rf.out$y==1))),
  #      main = paste0("Model ", model.no))
  # plot(AUC::roc(rf.out$votes[,"0"], factor(1*(rf.out$y==0))),
  #      add = T, col = "red")
  # AUC::auc(model_auc2)
   
  # save output predictions. Will need to re-work for non-binary outcomes
  if(is.null(test.dat)) { userows = testrows } else { userows = 1:nrow(test.dat.use) }# use testrows if 70/30 or all rows if separate train and test dat
  
  out.df <- data.frame(test.dat.use[userows, c("GRID_ID", "day", "hour", response.var)], rf.pred)
  out.df$day <- as.numeric(out.df$day)
  names(out.df)[4:5] <- c("Obs", "Pred")
  out.df = data.frame(out.df,
                      TN = out.df$Obs == 0 &  out.df$Pred == 0,
                      FP = out.df$Obs == 0 &  out.df$Pred == 1,
                      FN = out.df$Obs == 1 &  out.df$Pred == 0,
                      TP = out.df$Obs == 1 &  out.df$Pred == 1)
  write.csv(out.df,
            file = paste(model.no, "RandomForest_pred.csv", sep = "_"),
            row.names = F)
  
  savelist = c("rf.out", "rf.pred", "out.df") 
  if(is.null(test.dat)) savelist = c(savelist, "testrows", "trainrows")
  if(!is.null(thin.dat)) savelist = c(savelist, "test.dat.use")
     
  s3save(list = savelist,
     object = file.path(outputdir, paste("Model", model.no, "RandomForest_Output.RData", sep= "_")),
     bucket = waze.bucket)
  
  # Output is list of three elements: Nobs data frame, predtab table, binary model diagnotics table, and mean squared error
  list(Nobs, predtab, diag = bin.mod.diagnostics(predtab), 
       mse = mean(as.numeric(as.character(test.dat.use[,response.var])) - 
                as.numeric(as.character(rf.pred)))^2,
       runtime = timediff
       , auc = model_auc
  ) 
  
} # end do.rf function



# # Model 01
# # Test arguments
# # Variables to test. Use Waze only predictors, and omit grid ID and day as predictors as well. Here also omitting precipitation and neighboring grid cells
# # Omit as predictors in this vector:
# omits = c(grep("GRID_ID", names(w.04), value = T), "day", "hextime", "year", "weekday",
#           "uniqWazeEvents", "nWazeRowsInMatch", "nWazeAccident",
#           "nMatchWaze_buffer", "nNoMatchWaze_buffer",
#           grep("EDT", names(w.04), value = T),
#           "wx",
#           grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
#           grep("nWazeJam_", names(w.04), value = T) # neighboring jams
#           )
# 
# modelno = "01"
# train.dat = w.04[sample(1:nrow(w.04), size = 10000),]
# test.dat = NULL
# response.var = "MatchEDT_buffer_Acc"
# rf.inputs = list(ntree.use = avail.cores * 50, avail.cores = avail.cores, mtry = NULL, maxnodes = NULL)
# 
# do.rf(train.dat = w.04, omits, response.var = "MatchEDT_buffer_Acc", 
#       model.no = "01", rf.inputs = rf.inputs) 
