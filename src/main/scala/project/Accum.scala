package project

import org.apache.spark.sql.SparkSession
object Accum extends App{
  val spark = SparkSession.builder()
    .appName("adharproject")
    .config("spark.master","local")
    .getOrCreate()


  val rdd1 =spark.sparkContext.textFile("src/main/resources/trendytech/sampledata")

  val myaccum = spark.sparkContext.longAccumulator("blank lines accumulator")

  rdd1.foreach(x => if (x=="") myaccum.add(1))

println(myaccum)

  spark.catalog.listTables().show()


}
