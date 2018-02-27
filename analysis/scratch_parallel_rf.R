# Parallelized random forests notes
# To compare possible implementations on ATA server. Ordered in priority of testing

# Test data: run first section of RandomForest_WazeGrid.R, up to section header "# Random forest sequential ----"
# Test push from SDC


# <<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>>
# randomForest + foreach
# This uses the most widely-used implementation of random forests, along with the foreach pacakges. Not difficult to implement, and this should be the first option until shown to be not useful.



# <<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>>
# Caret
# Classification and regression in parallel
install.packages("caret", dependencies = c("Depends", "Suggests"))


# <<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>>
# H2O
# Potentially good for us, because implement a variety of machine learning task in a parallel way, including glm, random forest, and neural network. Large package.
# Open source math engine for big data, see h20.ai

# install.packages("h2o", dep = T)

# <<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>>
# XGBoost
# Variables must be numeric
# install.packages('xgboost', dependencies = T)

library(xgboost)

# <<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>>
# Ranger
# install.packages('ranger', dependencies = T)

library(xgboost)

# <<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>>
# ParallelForest




# Older packages
# Archived package bigrf, no longer on CRAN
# install.packages('bigrf')
