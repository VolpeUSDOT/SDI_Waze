# this function is used to extract diagnosis parameters and coefficients of the linear model and logistic model

Poisson_model_summary <- function(model_list, out.name){
  
  N <- 0 # total number of variables
  vec <- c(0) # vector to store the number of variable in each model
  nvec <- vector() # upper index
  mvec <- vector() # lower index
  for (i in 1:length(model_list)){
    ni <- nrow(summary(model_list[[i]])$coef)
    vec <- cbind(vec, ni)
    N <- ni + N
    nvec <- cbind(nvec, sum(vec[1:i+1])) # upper index
    mvec <- cbind(mvec, sum(vec[1:i]) + 1) # lower index
  }
  
  M = data.frame(matrix(NA,N,8))
  vec1 <- vec[2:(length(model_list)+1)]
  
  colnames(M) <- c("Model","Formula","Data", "N_Obs","AIC", "Variable", "Exp_coef", "vif")
  
  for(i in 1:length(model_list)){
    lm <- model_list[[i]]
    ni <- nrow(summary(lm)$coef)
    n <- nvec[i] # upper index
    m <- mvec[i] # lower index
    M[m:n,1] = row.name[i]
    M[m:n,2] = paste(format(formula(lm)), collapse = "") # paste(eval(lm$call[[2]]), collapse = "T") # formula
    M[m:n,3] = ifelse(length(lm$call[[3]]) > 1, paste(format(lm$call[[3]]), collapse = ""), paste(lm$call[[4]])) # data
    M[m:n,4] = length(fitted(lm)) # N of obs
    M[m:n,5] = round(summary(lm)$aic,2) # get AIC
    for (k in 1:ni){
      M[m+k-1, 6] = rownames(summary(lm)$coef)[k] # names of variables
      p_value = ifelse(summary(lm)$coef[k,4] < 0.05, "*","") # p-value, whether the summary will show * for significance at 95%
      M[m+k-1, 7] = paste0(round(exp(summary(lm)$coef[k,1]),2), p_value) # convert coefficients to odds ratio
    }
    for (j in 2:ni){
      M[m+j-1, 8] = ifelse(nrow(summary(lm)$coef)<=2, NA, paste0(round(vif(lm)[j-1],4)))
    }
  }
  
  model_summary <- reshape(M[, 1:7], timevar = c("Variable"), idvar = c("Model", "Formula", "Data", "AIC"), direction = "wide")
  
  model_compare <- unique(M[, 1:4])
  # fun_vif <- function(x){ifelse(x > 2, T, F)}
  # 
  # model_compare <- model_summary %>% replace(is.na(.), 0) %>% mutate(sum_all = rowSums(.[grep("vif", colnames(.))], na.rm = T))
  #   
  #   summarize_all(multicollinarity = funs(mean))
  # 
  #   mutate_at(.vars = grep("vif", colnames(model_summary), value = T), .funs = list(multicollinarity = ~fun_))
  #   
  #   summarize_at(.vars = grep("vif", colnames(model_summary), value = T), .funs = fun_vif(.))
  # 
  # # multicollinarity = ifelse(any(grep("vif", colnames(model_summary), value = T)) > 2, "Yes", "No"))
  
  Poisson_model_summary_list <- list("model_summary" = model_summary, "M" = M, "model_compare" = model_compare)
  save(list = c("Poisson_model_summary_list"), file = out.name)
  
  Poisson_model_summary_list
  # write.csv(model_summary, file = paste0("model_summary.csv"))
  
}


logistic_model_summary <- function(model_list, out.name){

  N <- 0 # total number of variables
  vec <- c(0) # vector to store the number of variable in each model
  nvec <- vector() # upper index
  mvec <- vector() # lower index
  for (i in 1:length(model_list)){
    ni <- nrow(summary(model_list[[i]])$coef)
    vec <- cbind(vec, ni)
    N <- ni + N
    nvec <- cbind(nvec, sum(vec[1:i+1])) # upper index
    mvec <- cbind(mvec, sum(vec[1:i]) + 1) # lower index
  }
  
  M = data.frame(matrix(NA,N,7))
  vec1 <- vec[2:(length(model_list)+1)]
  
  colnames(M) <- c("Model","Formula","Data","AIC","Variable","OR", "vif")
  
  for(i in 1:length(model_list)){
    lm <- paste0("Logistic-", model_list[[i]])
    ni <- nrow(summary(lm)$coef)
    n <- nvec[i] # upper index
    m <- mvec[i] # lower index
    M[m:n,1] = row.name[i]
    M[m:n,2] = paste(format(formula(lm)), collapse = "") # paste(eval(lm$call[[2]]), collapse = "T") # formula
    M[m:n,3] = ifelse(length(lm$call[[3]]) > 1, paste(format(lm$call[[3]]), collapse = ""), paste(lm$call[[4]])) # data
    M[m:n,4] = round(summary(lm)$aic,2) # get AIC
    for (k in 1:ni){
      M[m+k-1, 5] = rownames(summary(lm)$coef)[k] # names of variables
      p_value = ifelse(summary(lm)$coef[k,4] < 0.05, "*","") # p-value, whether the summary will show * for significance at 95%
      M[m+k-1, 6] = paste0(round(exp(summary(lm)$coef[k,1]),2), p_value) # convert coefficients to odds ratio
    }
    for (j in 2:ni){
      M[m+j-1, 7] = ifelse(nrow(summary(lm)$coef)<=2, NA, paste0(round(vif(lm)[j-1],4)))
    }
  }
  
  model_summary <- reshape(M[, 1:7], timevar = c("Variable"), idvar = c("Model", "Formula", "Data", "AIC"), direction = "wide")
  
  # model_compare <- unique(M[, 1:4])
  model_compare <- model_summary %>% group_by("Model", "Formula", "Data", "AIC") %>% summarize(multicollinarity = ifelse(any(vif) > 2, "Yes", "No"))
  
  logistic_model_summary_list <- list("model_summary" = model_summary, "M" = M, "model_compare" = model_compare)
  save(list = c("logistic_model_summary_list"), file = out.name)
    
  logistic_model_summary_list
  # write.csv(model_summary, file = paste0("model_summary.csv"))

}



linear_model_summary <- function(model_list, out.name){

  N <- 0 # total number of variables
  vec <- c(0) # vector to store the number of variable in each model
  nvec <- vector() # upper index
  mvec <- vector() # lower index
  for (i in 1:length(model_list)){
    ni <- nrow(summary(model_list[[i]])$coef)
    vec <- cbind(vec, ni)
    N <- ni + N
    nvec <- cbind(nvec, sum(vec[1:i+1])) # upper index
    mvec <- cbind(mvec, sum(vec[1:i]) + 1) # lower index
  }
  
  M = data.frame(matrix(NA,N,7))
  vec1 <- vec[2:(length(model_list)+1)]
  
  colnames(M) <- c("Model","Formula","Data","R-squared","Variable","Coef", "vif")
  
  for(i in 1:length(model_list)){
    lm <- paste0("OLS-", model_list[[i]])
    ni <- nrow(summary(model_list[[i]])$coef)
    n <- nvec[i] # upper index
    m <- mvec[i] # lower index
    M[m:n,1] = row.name[i]
    M[m:n,2] = paste(format(formula(lm)), collapse = "") # paste(eval(lm$call[[2]]), collapse = "T") # formula
    M[m:n,3] = ifelse(length(lm$call[[3]]) > 1, paste(format(lm$call[[3]]), collapse = ""), paste(lm$call[[3]])) # data
    M[m:n,4] = round(summary(lm)$r.squared,2)
    for (k in 1:ni){
      M[m+k-1, 5] = rownames(summary(lm)$coef)[k]
      p_value = ifelse(summary(lm)$coef[k,4] < 0.05, "*","")
      M[m+k-1, 6] = paste0(round(summary(lm)$coef[k,1],4), p_value)
    }
    for (j in 2:ni){
      M[m+j-1, 7] = ifelse(nrow(summary(lm)$coef)<=2, NA, paste0(round(vif(lm)[j-1],4)))
    }
  }
  
  model_summary <- reshape(M[, 1:6], timevar = c("Variable"), idvar = c("Model", "Formula", "Data", "R-squared"), direction = "wide")
  
  # model_compare <- unique(M[, 1:4])
  model_compare <- model_summary %>% group_by("Model", "Formula", "Data", "R-squared") %>% summarize(multicollinarity = ifelse(any(vif) > 2, "Yes", "No"))
  
  linear_model_summary_list <- list("model_summary" = model_summary, "M" = M, "model_compare" = model_compare)
  save(list = c("linear_model_summary_list"), file = out.name)
  
  linear_model_summary_list
  # write.csv(model_summary, file = paste0("model_summary.csv"))
}

