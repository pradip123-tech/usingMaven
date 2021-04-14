package finalpractice

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

object arrayTypes extends App {

  val spark = SparkSession.builder()
    .config("spark.master","local")
    .appName("arraytypes")
    .getOrCreate()




  val arrayStructureData = Seq(
    Row("James,,Smith",List("Java","Scala","C++"),List("Spark","Java"),"OH","CA"),
    Row("Michael,Rose,",List("Spark","Java","C++"),List("Spark","Java"),"NY","NJ"),
    Row("Robert,,Williams",List("CSharp","VB"),List("Spark","Python"),"UT","NV")
  )

  val arrayStructureSchema = new StructType()
    .add("name",StringType)
    .add("languagesAtSchool", ArrayType(StringType))
    .add("languagesAtWork", ArrayType(StringType))
    .add("currentState", StringType)
    .add("previousState", StringType)

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
  df.printSchema()
  df.show()


  //EXPLODE FUNCTION
import org.apache.spark.sql.functions._
  val df1=  df.select(col("name"),explode(col("languagesAtWork")) as("Language"))
df1.show()



}
