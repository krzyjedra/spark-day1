import org.apache.spark.sql.SparkSession

object Thu extends App {
  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .master("local[*]")
    .getOrCreate()

  spark.read.format("text").load("README.md")

  spark.read.text("README.md")

  val ds = spark.read.text("README.md")
    :type ds
}
