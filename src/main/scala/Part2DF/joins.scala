package Part2DF

import org.apache.spark.sql.SparkSession

object joins extends App {

  val spark = SparkSession.builder()
    .appName("DF joins")
    .config("spark.master","local")
    .getOrCreate()

   val bandsDF = spark.read
     .option("inferSchema", "true")
     .json("src/main/resources/Data/bands.json")

   val guitaristDF = spark.read
     .option("inferSchema", "true")
     .json("src/main/resources/Data/guitarPlayer.json")


  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/Data/guitars.json")

  //inner join
 val joinCondition = bandsDF.col("id") === guitaristDF.col("band")
   val guitaristBandDF = bandsDF.join(guitaristDF,joinCondition , "inner")

  guitaristBandDF.show()

  bandsDF.join(guitaristDF, joinCondition, "left_outer").show()

  bandsDF.join(guitaristDF, joinCondition,"right_outer").show()
  bandsDF.join(guitaristDF, joinCondition,"right_outer").show()
  bandsDF.join(guitaristDF, joinCondition,"right_outer").show()

  bandsDF.join(guitaristDF, bandsDF.col("id") === guitaristDF.col("band"), "outer").show()


guitaristBandDF.explain(true)
}
