package Part2DF

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions.{col, column, expr }
object ColumAndExpression extends App {

  val spark = SparkSession.builder()
    .appName("DF Column And Expression")
    .config("spark.master","local")
        .getOrCreate()


  val carDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/Data/cars")

  val firstColumn = carDF.col("Name")

  val carNames = carDF.select(firstColumn)

carNames.show()
import spark.implicits._
   val SelectingMethod = carDF.select(
     carDF.col("Name"),
     col("Acceleration"),
     column("weight_in_lbs"),
     'Year,  //Auto converted to column
     $"Horsepower",  //returns a coiumn
     expr("Origin")
   )
     val weightinkg = carDF.col("weight_in_lbs") / 2.2
    val carsWithWeightDF = carDF.select(
      col("Name"),
      col("weight_in_lbs"),
      weightinkg.as("Weight_1"),
      expr("weight_in_lbs / 2.2").as("Weight_2")
    )

    carsWithWeightDF.show()
val AllCountries = carDF.select("Origin").distinct()

AllCountries.show()

  val filterDF = carDF.filter(col("Origin") === "USA" && col("Horsepower") > 150)
filterDF.show()
}
 