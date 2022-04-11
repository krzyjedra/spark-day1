

object SparkEx2JL extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val input = Seq(
    Seq("a", "b", "c"),
    Seq("X", "Y", "Z")).toDF
  val array_size = 3
  (0 until array_size).foldLeft(input) { (result, n) => result.withColumn(s"$n", $"value"(n)) }
    .drop("value")
    .show(false)
}