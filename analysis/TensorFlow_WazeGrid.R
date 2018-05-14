# Keras/TensorFlow test

# requires python-virtualenv installed; sudo yum install or apt-get install as appropriate.

# Check these messages -- seems to work without:
# cloud-init 0.7.6 requires argparse, which is not installed.
# cloud-init 0.7.6 requires cheetah, which is not installed.
# cloud-init 0.7.6 requires oauth, which is not installed.
# cloud-init 0.7.6 requires PrettyTable, which is not installed.
# cloud-init 0.7.6 requires pyserial, which is not installed.
# jinja2 2.7.2 requires markupsafe, which is not installed.
# paramiko 1.15.1 requires pycrypto!=2.4,>=2.1, which is not installed

if(length(grep("keras", installed.packages()))==0){
  devtools::install_github('rstudio/keras')
  library(keras)
  install_keras() # runs TensorFlow as backend by default
}


# Run MNIST handwriting recognition example
library(keras)

mnist <- dataset_mnist()

x_train <- mnist$train$x
y_train <- mnist$train$y

head(x_train); dim(x_train)
head(y_train); dim(y_train)


x_test <- mnist$test$x
y_test <- mnist$test$y

#3D array of grayscale values: images, width, height. Reshape to 2D, making width and height one dimension using array_reshape()

x_train <- array_reshape(x_train, c(nrow(x_train), 28*28))
x_test  <- array_reshape(x_test, c(nrow(x_test), 28*28))

# Rescale to continuous value instead of integer in grayscale
x_train <- x_train/255
x_test <- x_test/255

# Prepare response variable as factor with 10 levels, 0-9

y_train <- to_categorical(y_train, 10)
y_test <- to_categorical(y_test, 10)

# Define the model ---
# Sequential model, a linear stack of layers

model <- keras_model_sequential()
model %>%
  layer_dense(units = 256, activation = 'relu', input_shape = c(784)) %>%
  layer_dropout(rate = 0.4) %>%
  layer_dense(units = 128, activation = 'relu') %>%
  layer_dropout(rate = 0.3) %>%
  layer_dense(units = 10, activation = 'softmax')

summary(model)

# Compile and run the model ---
model %>%  compile(
  loss = 'categorical_crossentropy',
  optimizer = optimizer_rmsprop(),
  metrics = c('accuracy')
  )

history <- model %>% 
  fit(x_train, y_train,
  epochs = 30, batch_size = 128,
  validation_split = 0.2)

# Evaluate and Show performance ----

plot(history)

model %>% evaluate(x_test, y_test)

pred.vals <- model %>%  predict_classes(x_test)

y_test_obs <- apply(y_test, 1, function(x) which(x == 1)) - 1

pred.obs <- data.frame(pred.vals, y_test_obs)
head(pred.obs)
plot(jitter(pred.vals) ~ jitter(y_test_obs),
     data = pred.obs,
     main = "MNIST handwriting classification \n by Keras/TensorFlow",
     pch = 16,
     col = scales::alpha("midnightblue", 0.05))

