package com.excersice.spark

import org.apache.spark.sql.SparkSession

object DataFrameBasic extends App {

  val spark = SparkSession.builder()
    .appName("DataFrame Basic Excercise")
    .config("spark.master","local")
    .getOrCreate()

  val smartphones = Seq(
    ("samsung","A20",110,15),
    ("oppo","c20",110,205),
    ("mi","A20",110,15),
    ("moto","A28",120,15),
    ("sony","k20",110,65),
    ("vivo","s20",210,25)
  )
    import spark.implicits._

  val manualsmartphones = smartphones.toDF("company","model","Dia","MP")
  manualsmartphones.show()
  manualsmartphones.printSchema()

  val df2 = spark.read
    .format("json")
    .option("inferSchema","true")
    .load("src/main/resources/Data/movies")

  df2.show()
  println(s"The Movie DF has ${df2.count()} rows")
}
