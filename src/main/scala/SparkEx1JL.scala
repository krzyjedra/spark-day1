import org.apache.spark.sql.functions.{concat, expr, lit, struct}

object SparkEx1JL extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val dept = Seq(
    ("50000.0#0#0#", "#"),
    ("0@1000.0@", "@"),
    ("1$", "$"),
    ("1000.00^Test_string", "^")).toDF("VALUES", "Delimiter")

  // working too:
  //    dept.withColumn("Delimiter",concat(lit("\\"),$"Delimiter"))
  //    .withColumn("split_values",expr("split(VALUES, Delimiter)"))
  //    .show(false)

  dept
    .withColumn("split_values", struct("VALUES", "Delimiter"))
    .withColumn("split_values", expr("""split(VALUES, concat("\\", Delimiter))"""))
    .show(false)
}