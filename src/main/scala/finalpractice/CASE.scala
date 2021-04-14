package finalpractice

import org.apache.spark.sql.{SparkSession, types}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
object CASE extends App {



    val spark = SparkSession.builder()
      .appName("case")
      .config("spark.master","local")
      .getOrCreate()

 val schema = StructType(Array(
   StructField("date", DateType),
     StructField("time",IntegerType),
     StructField("sex",StringType),
     StructField("confirmed",IntegerType),
     StructField("deceases",IntegerType)
 )

 )

    val caseDF= spark.read
      .option("header","true")
      .option("sep","\t")
      .option("inferSchema","true")
      .csv("src/main/resources/Data/covid-19/patientinfo")

    caseDF.show()
//caseDF.show()
caseDF.select(max("age")).show()
}
