import org.apache.spark.sql.functions._

object Fri6 extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val dataframe = spark
    .read
    .option("delimiter", ",")
    .option("header", true)
    .csv("Fri6.csv")

  val UpCCity = dataframe.withColumn("upper_city", upper($"city"))
  UpCCity.show

}
