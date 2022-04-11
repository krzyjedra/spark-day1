import org.apache.spark.sql.functions._

object Fri4 extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val dates = Seq("08/11/2015", "09/11/2015", "09/12/2015").toDF("date_string")

  val newDate = dates.withColumn("new_dates", to_date($"date_string", "dd/MM/yyyy"))
  val diff = newDate.withColumn("diff", datediff(current_date(), $"new_dates"))
  diff.show

}
