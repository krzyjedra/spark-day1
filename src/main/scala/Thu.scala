object Thu extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()

  //  spark.read.format("text").load("README.md")
  //  spark.read.text("README.md")
  //  val ds = spark.read.text("README.md")

  import spark.implicits._

  (0 to 5).toDF("cokolwiek")

  val numbers = Seq(Seq(1, 2, 3)).toDF("numbers")

  numbers.flatMap { r =>
    val ns = r.getSeq[Int](0)
    ns.map(n => (ns, n))
  }.toDF("nums", "num").show(false)
}
