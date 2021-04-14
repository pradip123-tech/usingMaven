package finalpractice

import org.apache.spark.sql.SparkSession

object groupby extends App {

  val spark = SparkSession.builder()
    .appName("groupby")
    .config("spark.master","local")
    .getOrCreate()


  val simpleData = Seq(("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  )
import spark.implicits._
  val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")
  //df.show()


 val df1= df.groupBy("department").sum("salary")
  //df1.show()

  val df2 = df.groupBy("department").count()
  //df2.show()

  

}
