library(SparkR)
library(ggplot2.SparkR)
## sudo rstudio-server restart

Sys.setenv(SPARK_HOME = "/home/cloudera/spark")

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sc <- sparkR.init(master = "local[*]", sparkEnvir = list(spark.driver.memory="2g"))
sc <- sparkR.init()

## load data
data(iris);iris
cameras <- read.csv("/home/cloudera/Dropbox/cameras.csv")
customer_info <- read.csv("/home/cloudera/Dropbox/customer_info.csv")

#Creating DataFrames
  # From local data frames
  sqlContext <- sparkRSQL.init(sc)
  df <- createDataFrame(sqlContext, customer_info)
  head(df)
  
### Selecting rows, columns
  # You can also pass in column name as strings
  head(select(df, "occupation"))
  # Filter the DataFrame to only retain rows with wait times shorter than 50 mins
  head(filter(df, df$age<35 ))
  
###Grouping, Aggregation
  # We use the `n` operator to count the number of times each waiting time appears
  gender_count <- summarize(groupBy(df, df$gender), count = n(df$gender))
  age_count <- summarize(groupBy(df, df$age), count = n(df$age))
  result <- arrange(age_count, desc(age_count$age))
  head(result)

###Operating on Columns
  df$test <- df$age+20
  dsadfd
  
####### Machine Learning
  ##Gaussian GLM model
    # Create the DataFrame
    df <- createDataFrame(sqlContext, iris)
    # Fit a gaussian GLM model over the dataset.
    model <- glm(Sepal_Length ~ Sepal_Width + Species, data = df, family = "gaussian")
    # Model summary are returned in a similar format to R's native glm().
    summary(model)
    # Make predictions based on the model.
    predictions <- predict(model, newData = df)
    head(select(predictions, "Sepal_Length", "prediction"))
    
  
  ###Binomial GLM model
    # Create the DataFrame
    df <- createDataFrame(sqlContext, iris)
    training <- filter(df, df$Species != "setosa")
    
    # Fit a binomial GLM model over the dataset.
    model <- glm(Species ~ Sepal_Length + Sepal_Width, data = training, family = "binomial")
    
    # Model coefficients are returned in a similar format to R's native glm().
    summary(model)
    

    
    
  # Fomr HIVE: sc is an existing SparkContext.
  hiveContext <- sparkRHive.init(sc)
  # Queries can be expressed in HiveQL.
  results <- sql(hiveContext, "SELECT * FROM employees limit 5")



sparkR.stop()






