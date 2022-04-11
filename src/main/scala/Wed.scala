
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
  val dataframe = spark
    .read
    .option("delimiter", ",")
    .option("header", "true")
    .csv("sample2.csv")

//  val dfUpCase = dataframe
//    .withColumn("Name", upper(dataframe("Name")))

//  dataframe.show
//  val firstMethod = dataframe
//    .filter(dataframe("Name") === "Paul Bako")


//  val secondMethod = dataframe
//    .withColumn("TeamPosition", concat(col("\"Team\""), lit(','), col("\"Position\"")))

//  val thirdMethod = dataframe.groupBy("Team").avg("Age").show
//  working too:
//  val fourthMethod = dataframe.groupBy("Team").count.show
  val fifthMethod = dataframe.groupBy("Team").sum("Age").toDF.show

//  dataframe
//    .select(concat(col("Team"), lit(','), col("Position"))
//      .as("TeamPos"))

  // Exercise: Use command line for the CSV to read
  // Jesli podano pojedynczy parametr na linii polecen, uzyj go jako sciezki do CSV
  // else uzyj domyslnej sciezki (np. ../datasets/people.csv)
  val path = if(args.length > 0) args(0) else "sample2.csv"
}
