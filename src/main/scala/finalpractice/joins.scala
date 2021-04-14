package finalpractice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object joins extends  App {

  val spark = SparkSession.builder()
    .appName("joins")
    .config("spark.master","local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/Data/guitars.json")

  val guitaristDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/Data/guitarPlayer.json")

  val bandDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/Data/bands.json")

  guitarsDF.show()
  guitaristDF.show()
  bandDF.show()

val joinedDF = guitaristDF.joinWith(bandDF,guitaristDF.col("band") === bandDF.col("id"),"inner")
  joinedDF.show()
  joinedDF.explain()

  bandDF.filter("name == 'Metallica'" )
  
}
