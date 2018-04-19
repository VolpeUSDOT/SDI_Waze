# Random Forest functions for Waze

######################################################################
# do.rf function for running random forest models in parallel ----

# Arguments:
# train.dat - Data frame containing all the predictors and the response variable

# omits - Vector of column names in the data to omit from the model. Used to genearte the model formula, if the formula is not otherwise provided in the `formula` argument.

# response.var - Vector of the data to use as the response variable, e.g. MatchEDT_buffer_Acc or nMatchEDT_buffer_Acc

# model.no - character value to keep track of model number used; cannot be left blank

# formula - If provided, use this formula instead of the automatically generated one for the random forest model

# test.dat - If provided, this will be used to test the random forest model. If not provided, a split of the training data will be used according to the test.split argument. Both test.dat and test.split cannot be provided.

# test.split - if test.dat is not specified, use this to randomly split the training data into two portions by row, with this split value being used as the test proportion of the data.

# rf.inputs - list of arguments to pass to randomForest

do.rf <- function(train.dat, omits, response.var = "MatchEDT_buffer_Acc", model.no,
                  rf.formula = NULL, test.dat = NULL, test.split = .30,
                  rf.inputs = list(ntree.use = 500, avail.cores = 4, mtry = NULL, maxnodes = NULL, nodesize = 5)){
  
  starttime = Sys.time()
    
  if(!is.null(test.dat) & !missing(test.split)) stop("Specify either test.dat or test.split, but not both")

  class(train.dat) <- "data.frame"
  
  # If no formula provided, create one from names of training data - vector of omits. 
  # If formula provided, extract the RHS of formula as the fitvars vector of predictor names
  if(is.null(rf.formula)){
      fitvars <- names(train.dat)[is.na(match(names(train.dat), omits))]
      rf.formula <- reformulate(termlabels = fitvars[is.na(match(fitvars, response.var))], response = response.var)
    } else { 
      fitvars = attr(terms.formula(rf.formula), "term.labels")
    }

  # Provide mtry if null
  if(is.null(rf.inputs$mtry)){
    mtry.use = if (!is.factor(response.var)) max(floor(length(fitvars)/3), 1) else floor(sqrt(length(fitvars)))
  } else {mtry.use = rf.inputs$mtry}
  
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
   
  # cl <- makeCluster(parallel::detectCores()) # make a cluster of all available cores
  # registerDoParallel(cl) 
  
  rf.out <- foreach(ntree = rep(rf.inputs$ntree.use/rf.inputs$avail.cores, rf.inputs$avail.cores),
                  .combine = randomForest::combine, .multicombine=TRUE, .packages = 'randomForest') %dopar% 
            randomForest(rf.formula, data = rundat, ntree = ntree, mtry = mtry.use, maxnodes = rf.inputs$maxnodes, nodesize = rf.inputs$nodesize)

  timediff = Sys.time() - starttime
  cat(round(timediff,2), attr(timediff, "unit"), "to fit model", model.no, "\n")
  
  rf.pred <- predict(rf.out, test.dat.use[fitvars])

  Nobs <- data.frame(nrow(train.dat),
                 sum(as.numeric(train.dat[,response.var]) == 0),
                 sum(as.numeric(train.dat[,response.var]) > 0),
                 length(train.dat$nWazeAccident[train.dat$nWazeAccident>0]) )
  
  colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
  predtab <- table(test.dat.use[,response.var], rf.pred)

  # save output predictions. Will need to re-work for non-binary outcomes
  if(is.null(test.dat)) userows = testrows else userows = 1:nrow(train.dat) # use testrows if 70/30 or all rows if separate train and test dat
  
  out.df <- data.frame(train.dat[userows, c("GRID_ID", "day", "hour", response.var)], rf.pred)
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
  
  savelist = c("rf.out", "rf.pred", "train.dat", "out.df")
  if(is.null(test.dat)) savelist = c(savelist, "testrows", "trainrows")
     
  s3save(list = savelist,
     object = file.path(outputdir, paste("Model", model.no, "RandomForest_Output.RData", sep= "_")),
     bucket = waze.bucket)

  # Output is list of three elements: Nobs data frame, predtab table, and binary model diagnotics table
  list(Nobs, predtab, diag = bin.mod.diagnostics(predtab)) 
  
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
# response.var = "MatchEDT_buffer_Acc"
# avail.cores = parallel::detectCores()
# rf.inputs = list(ntree.use = avail.cores * 50, avail.cores = avail.cores, mtry = NULL, maxnodes = NULL)
# 
# do.rf(train.dat = w.04, omits, response.var = "MatchEDT_buffer_Acc", 
#       model.no = "01", rf.inputs = rf.inputs) 
