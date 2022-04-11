object Test extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()

  val dataframe = spark
    .read
//  .option("delimiter", ",")
    .option("inferSchema", true)
    .option("header", true)
    .csv("file.csv")

  dataframe.groupBy("name").sum("age").show
  //  dataframe.groupBy("name").count.show
}
