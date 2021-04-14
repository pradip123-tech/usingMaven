package Part2DF

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column}
object ColumnAndExpressionExcercise extends App {

   val spark = SparkSession.builder()
     .appName("Excercise")
     .config("spark.master", "local")
     .getOrCreate()

  val movieDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/Data/movies")

  val exc1 = movieDF.select(
col("Title"),
    column("US_Gross")
  )
exc1.show()

  val exc2 = movieDF.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),

    col("US_DVD_Sales"),
    (col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross")
  )
  exc2.show()

  val exc4 = movieDF.withColumn("Total_Gross", col("US_Gross") + col("Worldwide_Gross") )
  exc4.show()


  val exc3 = movieDF.filter(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)
  exc3.show()

 val comediesDF = movieDF.select("Title","IMDB_Rating")
  .where(col("Major_Genre") ==="Comedy" and col("IMDB_Rating") > 6)

  val comediesDF2 = movieDF.select("Title","IMDB_Rating")
    .where("Major_Genre = 'Comedy' and IMDB_Rating >6")

}
