package Part2DF

import Part2DF.FirstSpark.{carsDFSchema, spark}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Format")
    .config("spark.master","local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsSchema)
    .load("src/main/resources/data/cars")


    carsDFWithSchema.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/Data/cars_dup")


   val stockschemas = StructType(Array(
     StructField("symbol",StringType),
     StructField("Date",DateType),
     StructField("price", DoubleType)
   ))



   val csvDF = spark.read
       .schema(stockschemas)
       .option("dateFormat","MMM DD YYYY")
       .option("header","true")
       .option("sep",",")
       .option("nullValue","")
       .csv("src/main/resources/Data/stocks.csv")


   csvDF.show()

   csvDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/Data/CSV_PARQUET")





}
