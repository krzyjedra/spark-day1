object FirstSparkApp extends App {
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .master("local[*]")
    .getOrCreate()

  val df = spark.read.text("build.sbt")

  df.show
  df.write.text("chkddsadsask.txt")
  spark.read.text("chkddsadsask.txt")
}
