package project

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object AdharDetails extends App {

  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("adharproject")
      .config("spark.master","local")
      .getOrCreate()

  val adharDF = spark.read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/Data/adhar")


  // val mahadf = adharDF.filter(col("State") === "Maharashtra" and col("District") === "Aurangabad")
//mahadf.show()
  //println(s"The df have ${mahadf.count()} rows")

  //adharDF.filter("District in ('Beed','Pune')").show()
adharDF.show()
  
 val MDF= adharDF.where("State like 'M%'").distinct()

println(s"The total states starting with M is ${MDF.distinct().count()}")

  val rejectedDF = adharDF.where(col("Enrolment Rejected") === 1 && col("Gender") === "M")

//  rejectedDF.show()

  println(s"The total no of application rejected = ${rejectedDF.count()}")

     //val duplicate = adharDF.dropDuplicates("District").selectExpr("count(District) as New_Districts").show()


     val orderdf = adharDF.orderBy(col("District").desc_nulls_last,col("Sub District")).show()




}