library(SparkR)
library(Matrix)
library(caret)
library(ggplot2.SparkR)
Sys.setenv(SPARK_HOME = "/home/cloudera/spark")
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sc <- sparkR.init(master = "local[*]", sparkEnvir = list(spark.driver.memory="2g"))

lines <- textFile(sc, "hdfs:/users/cloudera/data/shakespeare.txt")
