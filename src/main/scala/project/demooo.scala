package project

import org.apache.spark.sql.SparkSession

object demooo extends App{

  val spark = SparkSession.builder()
    .appName("pm")
    .config("spark.master","local")
    .getOrCreate()


  val df1 = spark.read.json("src/main/resources/Data/guitars.json")

   df1.show()

}
