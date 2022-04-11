import org.apache.spark.sql.functions._

object Fri3 extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val nums = Seq(Seq(1, 2, 3)).toDF("nums")
  nums.withColumn("num", explode(col("nums")))
    .show
}
