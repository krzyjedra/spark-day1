
object Wed extends App {

  import org.apache.spark.sql.SparkSession
  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .master("local[*]")
    .getOrCreate()
  // Morning Exercise
  // DataFrame (Dataset)
  // 1. Load CSV file
  // 2. Dataset.withColumn + functions object
  //  - https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html
  //  - Values should all be UPPER case
  // 3. DataFrame.show
  import org.apache.spark.sql.functions._
  val df = spark.read.option("delimiter", ",").option("header", "true").csv("sample2.csv")
  val df1 = df.withColumn("Name", upper(df("Name")))
 df1.show
}
