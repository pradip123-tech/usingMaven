package finalpractice

import org.apache.spark.sql.SparkSession
import org.apache.zookeeper.server.SessionTracker.Session
import java.sql.DriverManager
object mysql extends App {

   val spark = SparkSession.builder()
     .appName("mysql connectivity")
     .config("spark.master","local")
     .getOrCreate()


    val mysqlurl = "jdbc:mysql://localhost:3306/sakila"

  val filmDF = spark.read.format("jdbc").option("driver", "com.mysql.jdbc.Driver")
    .option("url", mysqlurl)
    .option("dbtable", "film")
    .option("user", "root")
    .option("password","root")
    .load()


  filmDF.show()

}
