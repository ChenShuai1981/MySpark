import org.apache.spark.sql.SparkSession

case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)

object HelloWorld extends App {
  val spark = SparkSession.builder().appName("").master("local").getOrCreate();
  val flightData2015 = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("/Users/chenshuai1/github/MySpark/src/main/resources/2015-summary.csv")
//  flightData2015.take(3)
  spark.conf.set("spark.sql.shuffle.partitions", "5")
  import org.apache.spark.sql.functions._
  import spark.implicits._
//  flightData2015.sort(desc("count")).take(2).foreach(println)

//  flightData2015.createOrReplaceTempView("flightData2015")
//  spark.sql("select * from flightData2015 order by count desc limit 2").show()

  val flightsDF = flightData2015
    .groupBy("DEST_COUNTRY_NAME")
    .sum("count")
    .withColumnRenamed("sum(count)", "destination_total")
    .sort(desc("destination_total"))
    .limit(5)
    .show()

  val flights = flightData2015.as[Flight]
  flights
    .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Croatia")
    .map(flight_row => flight_row)
    .take(5)
    .foreach(println)

  spark.stop()
}
