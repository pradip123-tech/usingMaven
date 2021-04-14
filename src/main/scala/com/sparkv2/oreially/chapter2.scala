package com.sparkv2.oreially

import org.apache.spark.sql.SparkSession
object chapter2  extends App {


  val spark = SparkSession.builder().
    appName("chapt2").
    config("spark.master","local").
    getOrCreate()

  val mnmDF = spark.read.format("csv").option("header","true").option("inferSchema","true").
    load("src/main/resources/Data/CSV_PARQUET/mnm_dataset").rdd

  //mnmDF.show(5)

  //val countMnMDF = mnmDF
    //.select(col("State"),col("Color"),col("Count"))


//mnmDF.show()
val dataDF = spark.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25),
  ("Denny", 31), ("Jules", 30), ("TD", 35))).toDF("name", "age")
  dataDF.show()

}
