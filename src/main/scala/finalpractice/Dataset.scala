package finalpractice


import org.apache.spark.sql._
import java.sql.Date


object Dataset extends  App {

          val spark = SparkSession.builder()
            .appName("Dataset")
            .config("spark.master","local")
            .getOrCreate()


  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: Date,
                  Origin: String
                )

  val carDF = spark.read
    .json("src/main/resources/Data/cars")

  import spark.implicits._
   val carDS = carDF.as[Car]


  carDS.select("Name").show()
carDS.map(x => x.Acceleration < 50).show()

  val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
    ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
    ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
    ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))

  val df3 =data.toDF("Product","Amount","Country")

df3.show()


}
