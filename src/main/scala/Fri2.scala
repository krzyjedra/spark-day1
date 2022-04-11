import org.apache.spark.sql.functions._

object Fri2 extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val words = Seq(Array("hello", "world")).toDF("words")
  val fullline = words.withColumn("words", concat_ws(" ", expr("words")))
  fullline.show
}
