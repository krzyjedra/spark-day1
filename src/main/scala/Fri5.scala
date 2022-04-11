import org.apache.spark.sql.functions._

object Fri5 extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val data = Seq(
    (0, "2016-01-1"),
    (1, "2016-02-2"),
    (2, "2016-03-22"),
    (3, "2016-04-25"),
    (4, "2016-05-21"),
    (5, "2016-06-1"),
    (6, "2016-03-21"))
    .toDF("number_of_days", "date")

  val future = data.withColumn("future", date_add($"date", $"number_of_days"))
  future.show
}
