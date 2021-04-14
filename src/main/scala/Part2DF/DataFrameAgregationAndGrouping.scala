package Part2DF

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object DataFrameAgregationAndGrouping extends App {

  val spark = SparkSession.builder()
    .appName("DFAggregaion and grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val movieDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/Data/movies")

val generCountDF = movieDF.select(count(col("Major_Genre")))
  generCountDF.show()
  movieDF.select(count("*")).show()


  val distantDF = movieDF.select(countDistinct(col("Major_Genre")))

  distantDF.show()

    val approxDF = movieDF.select(approx_count_distinct(col("Major_Genre")))
approxDF.show()

  movieDF.select(sum(col("IMDB_Rating")))


  //exc


  val exc1 = movieDF
    .select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
    .select(sum("Total_Gross"))

    exc1.show()

    val exc2 = movieDF.select(countDistinct("Director"))
    exc2.show()


  val exc3 = movieDF.select(
    mean(col("US_Gross")),
    stddev(col("US_Gross"))
  )
    exc3.show()


  val exc4 = movieDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_Rating"),
      sum("US_Gross").as("Total_US_Gross")
    )
    .orderBy(col("Avg_Rating").desc_nulls_last)




}
