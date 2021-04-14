package finalpractice

import org.apache.spark.sql.SparkSession
object sample extends App{

  val spark = SparkSession.builder()
    .appName("sample")
    .config("spark.master","local")
    .getOrCreate()
  import spark.implicits._
    val df = Seq(("jem",1,"abad"),
      ("pm",2,"pune"))

  val df2 = df.toDF("name","id","city")
  df2.show()

}
