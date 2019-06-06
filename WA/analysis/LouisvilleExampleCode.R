## 4.1 Feature Selection with Boruta

# run Boruta analysis
boruta.train <- Boruta(Count_2017 ~ ., data = training %>% as.data.frame %>% dplyr::select(built_env, usage, location, network, misc), doTrace = 0)

# save selected features
borutaVars <- getSelectedAttributes(boruta.train)

built_env <- c("TrafficSig", "ONEWAY", "BIKEWAY", "rank", "avg_width", "No_Left", "Stop_Sign", "Slow_Sign", "SpdLim_Sig", "Yield_Sign", "Discnt_Lne", "Frght_R", "potholes", "missign")

usage <- c("SPEED", "ParkingCit", "Cite_Dens", "volume", "hevtraff", "medtraff", "stndtraff", "lev1j", "lev2j", "lev3j", "lev4j", "hazobj",  "rdkill")

location <- c("Dist_Alcoh", "ZONING_COD", "DISTRICT", "POP_DEN", "BSNS_DENS", "Emply_D", "Shop_Dn", "Downt_Dist", "NearArtery", "GEOID")

network <- c("Intersect", "TYPE", "N_NEIGHBS", "Var_SPEED", "Length")

dont_include <- c("COLS_MILE", "Count_2016", "geometry", "FID_1", "Count_2017")

## 4.2 Multi-colinearity

library(dplyr)

segments <- st_read("segments_3_21.shp")

segments <- segments %>% dplyr::select(built_env, usage, location, network, Count_2017)

## identify Numeric Feature Names
num_features <- names(which(sapply(segments, is.numeric)))
segments_num <- segments %>% as.data.frame %>% dplyr::select(num_features)

#Correlation Matrix
CorMatrix <- cor(segments_num)

library(corrplot)

corrplot(CorMatrix, method = "color", order = "AOE", addCoef.col="grey", addCoefasPercent = FALSE, number.cex = .7)

library(FactoMineR)
library(factoextra)

waze_vars <- subset(as.data.frame(segments), select = c('lev3j', 'lev1j', 'lev2j', 'lev4j', 'missign', 'rdkill', 'potholes', 'hevtraff', 'medtraff', 'stndtraff', 'hazobj', 'volume'))

waze_usage.pca <- PCA(waze_vars, scale.unit = TRUE, ncp = 2)

eig.val <- get_eigenvalue(waze_usage.pca)

segments <- cbind.data.frame(segments,waze_usage.pca$ind$coord)
segments <- segments %>% dplyr::select(-lev3j, -lev1j, -lev2j, -lev4j, -missign, -rdkill, -potholes, -hevtraff, -medtraff, -stndtraff, -hazobj, -volume)

names(segments)[names(segments) == 'Dim.1'] <- 'waze_use_1'
names(segments)[names(segments) == 'Dim.2'] <- 'waze_use_2'
segments$Row.names <- NULL

library(kableExtra)

built_env <- c("TrafficSig", "ONEWAY", "BIKEWAY", "rank", "avg_width", "No_Left", "Stop_Sign", "Slow_Sign", "SpdLim_Sig", "Yield_Sign", "Discnt_Lne", "Frght_R")

usage <- c("SPEED", "ParkingCit", "Cite_Dens", "waze_use_1", "waze_use_2")

location <- c("Dist_Alcoh", "ZONING_COD", "DISTRICT", "POP_DEN", "BSNS_DENS", "Emply_D", "Shop_Dn", "Downt_Dist", "NearArtery", "GEOID")

network <- c("Intersect", "TYPE", "N_NEIGHBS", "Var_SPEED", "Length")


# 5 Model Selection

## 5.1 Set-up

set.seed(888)
segments <- segments %>% as.data.frame %>% dplyr::select(built_env, usage, location, network, Count_2017)

partition <- createDataPartition(y=segments$GEOID ,p=.80,list=F)
training <- segments[partition,]
testing <- segments[-partition,]

## 5.2 Prediction Baseline

Type_Averages <- aggregate(Count_2017~TYPE+GEOID, segments, mean)
colnames(Type_Averages) <- c("TYPE", "GEOID", "Type_Est")

segment_baseline <- merge(x = segments, y = Type_Averages, by = c("GEOID","TYPE"))

baseline <- mae(segment_baseline$Type_Est, segment_baseline$Count_2017)


## 5.3 Model Validation

fit_ols <- lm(Count_2017 ~ ., data = training)

y_hat_ols <- predict(fit_ols,testing)
ols_mae <- mae(testing$Count_2017, y_hat_ols)

#Poisson
fit_poisson <- glm(Count_2017 ~ ., data = training, family="poisson")

y_hat_poisson <- predict(fit_poisson,testing, type = "response")

pois_mae <- mae(testing$Count_2017, y_hat_poisson)


## Try Ridge
fit.ridge.cv <- cv.glmnet(data.matrix(training %>% as.data.frame %>% dplyr::select(-Count_2017)), training$Count_2017, alpha=0,type.measure="mae")

y_hat_l2 <- predict.cv.glmnet(fit.ridge.cv,data.matrix(testing %>% as.data.frame %>% dplyr::select(-Count_2017)))

ridge_mae <- mae(testing$Count_2017, y_hat_l2)

## Try Lasso

fit.lasso.cv <- cv.glmnet(data.matrix(training %>% as.data.frame %>% dplyr::select(-Count_2017)), training$Count_2017, alpha=1,type.measure="mae")

y_hat_l1 <- predict.cv.glmnet(fit.lasso.cv,data.matrix(testing %>% as.data.frame %>% dplyr::select(-Count_2017)))

las_mae <- mae(testing$Count_2017, y_hat_l1)

## Try Elastic Net

fit.net.cv <- cv.glmnet(data.matrix(training %>% as.data.frame %>% dplyr::select(-Count_2017)), training$Count_2017, alpha=0.5,type.measure="mae")

y_hat_l1l2 <- predict.cv.glmnet(fit.net.cv,data.matrix(testing %>% as.data.frame %>% dplyr::select(-Count_2017)))

enet_mae <- mae(testing$Count_2017, y_hat_l1l2)

## Try CART
library(rpart)

fit_tree <- rpart(Count_2017 ~ .
                  , minsplit = 100
                  , maxdepth = 15
                  , data=training)

y_hat_tree <- predict(fit_tree,testing)

tree_mae <- mae(testing$Count_2017, y_hat_tree)

require(randomForest)

fit_rf30 <- randomForest(Count_2017 ~ .
                         , method="poisson"
                         , ntree = 30
                         , na.action = na.exclude
                         , data=training %>% 
                           as.data.frame %>% dplyr::select(-NearArtery, -GEOID))


y_hat_rf30 <- predict(fit_rf30,testing)

rf_mae <- mae(testing$Count_2017, y_hat_rf30)

library(xgboost)
set.seed(777)

XGB_data <- data.matrix(training %>% as.data.frame %>% dplyr::select(-Count_2017))

fit_xgb <- xgboost(XGB_data, training$Count_2017
                   , max_depth = 10
                   , eta = 0.05
                   , nthread = 2
                   , nrounds = 1000
                   , subsample = .7
                   , colsample_bytree = 0.9
                   , booster = "gbtree"
                   , eval_metric = "mae"
                   , objective="count:poisson"
                   , base_score = 0.56
                   , silent = 1)

y_hat_xgb <- predict(fit_xgb, data.matrix(testing %>% as.data.frame))

xgb_mae <- mae(testing$Count_2017, y_hat_xgb)

library(scales)

#storing the results
results <- data.frame()

base <- c("baseline", round(baseline,2), "N/A") 

ols <- c("OLS", round(ols_mae,2), percent((baseline-ols_mae)/baseline))
pois <- c("Poisson", round(pois_mae,2), percent((baseline-pois_mae)/baseline))
ridge <- c("Ridge", round(ridge_mae,2), percent((baseline-ridge_mae)/baseline))
lass <- c("Lasso", round(las_mae,2), percent((baseline-las_mae)/baseline))
elnet <- c("Elastic Net", round(enet_mae,2), percent((baseline-enet_mae)/baseline))
tree <- c("CART", round(tree_mae,2), percent((baseline-tree_mae)/baseline))
rf <- c("Random Forest", round(rf_mae,2), percent((baseline-rf_mae)/baseline))
xgb <- c("XGBoost", round(xgb_mae,2), percent((baseline-xgb_mae)/baseline))

results <- rbind(base, ols, pois, ridge, lass, elnet, tree, rf, xgb)
rownames(results) <- c()
colnames(results) <- c("Model", "Mean Absolute Error (MAE)", "Baseline Improvement")
