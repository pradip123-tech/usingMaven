package finalpractice

import com.esotericsoftware.minlog.Log.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object udemy  extends  App {

  val spark = SparkSession.builder()
    .appName("Dataset")
    .config("spark.master","local")
    .getOrCreate()

     case class flight(DEST_COUNTRY_NAME: String,
                       ORIGIN_COUNTRY_NAME: String, count:BigInt
                      )


     val df1 = spark.read
       .json("src/main/resources/Data/flight.data")
import spark.implicits._
  val ds1 = df1.as[flight]

  //ds1.show()
 // ds1.printSchema()
  ds1.select(avg(col("count")))
  val ds2 = ds1.map(x => x.count>50)
 // ds2.show()
//ds2.take(5)

  val df = spark.read
    .json("src/main/resources/Data/bands.json")
  //df.show()
//df.printSchema()
  //df.withColumn ("id",col("id").cast("String"))
  //df.printSchema()

  //2] change the value of an existing column===
  df.withColumn("id",col("id")+10).show()
  df.show()

  // 3]  Derive new colunm from existing column

 //df.withColumn("newcolumn",col("id")-1)

  //df.withColumnRenamed("id","ID").show()
  //df.withColumn("country",lit("usa")).show()



  val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
    ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
    ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
    ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))

  val df3 = data.toDF("product","Amount","Country")
  df3.show()

  val pivotDF = df3.groupBy("Product").pivot("Country").sum("Amount")
 // pivotDF.show()

  //println(s"The df having ${df3.count()} rows")

  val simpleData = Seq(Row("James","","Smith","36636","M",3000),
    Row("Michael","Rose","","40288","M",4000),
    Row("Robert","","Williams","42114","M",4000),
    Row("Maria","Anne","Jones","39192","F",4000),
    Row("Jen","Mary","Brown","","F",-1)
  )

  val simpleSchema = StructType(Array(
    StructField("firstname",StringType),
    StructField("middlename",StringType),
    StructField("lastname",StringType),
    StructField("id", StringType),
    StructField("gender", StringType),
    StructField("salary", IntegerType)
  ))

  val df6 = spark.createDataFrame(
    spark.sparkContext.parallelize(simpleData),simpleSchema)

  df6.printSchema()
//  df6.show()

//df6.createTempView("temp")

   //spark.catalog.listTables().show()

  //spark.sql("show tables").show()
 //spark.sql("select * from temp where firstname like 'J% ' ").show


val df7 = df6.describe()


  // df7.show()

 // df6.withColumn("Length",trim(col("firstname"))).show()
  df6.show()



}
