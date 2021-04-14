package project

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
object sampletest extends App {


  val spark = SparkSession.builder()
    .appName(" Nested xml file")
    .config("spark.master","local")
    .getOrCreate()
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

   val df1 = spark.read
     .format("xml")
     .option("rowTag", "node")
     .option("rootTag", "links")
     .option("rowTag", "link")
     .load("src/main/resources/Data/test")

df1.printSchema()
  df1.show()

  val df2 =  df1.selectExpr(" _capacity","_freespeed","_from","_id","_length","_modes","_oneway","_permlanes","_to").
    withColumn("exploded",explode(col("attributes")))
//df2.show()

  //df2.write.format("csv").save("src/main/resources/Data/output")
}
