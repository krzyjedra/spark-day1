import org.apache.spark.sql.functions._

object Fri extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val input = spark.range(50).withColumn("key", $"id" % 5)
  //  input.show
  val helper = input
    .groupBy("key")
    .agg(collect_set("id").as("all"))
    .withColumn("only_first_three", col("all"))

  val solution = helper
    .withColumn("only_first_three", filter($"only_first_three", (x, idx) => idx <= 2))
    .show

  // Seq(Seq("hello", "world")).toDF("words")
  // Seq((1,2,3)).toDF("one", "two", "three")
  // Seq((1, Seq(2))).toDF.printSchema
}

