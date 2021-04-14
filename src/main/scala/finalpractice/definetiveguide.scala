package finalpractice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object definetiveguide extends App {


  val spark = SparkSession.builder()
    .appName("guide" )
    .config("spark.master","local")
    .getOrCreate()


  val flightDF = spark.read
    .option("inferSchema","true")
    .option("header","true")

    .csv("E:\\Karunakar\\KARUNAKR DATA\\Spark\\DefinativeGuide\\Spark-The-Definitive-Guide-master\\data\\flight-data\\csv")

  flightDF.sort("count").explain()

flightDF.createTempView("flightDF")

  val topDF = spark.sql("select DEST_COUNTRY_NAME,sum(count) as destination_total from flightDF group by DEST_COUNTRY_NAME order by sum(count) desc limit(5)")
 topDF.show()

  flightDF.show()

  val dataDF = spark.read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/Data/stocks.csv")

  dataDF.show()


  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/Data/movies")

  // Dates

}

