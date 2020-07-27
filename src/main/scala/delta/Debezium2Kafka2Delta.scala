package delta

import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._

object Debezium2Kafka2Delta extends App {

  val schema = {
    val f1 = StructField("name", StringType, true)
    val f2 = StructField("reason", StringType, true)
    val f3 = StructField("duration_seconds", LongType, true)
    StructType(Seq(f1, f2, f3))
  }

  val spark = SparkSession.builder().appName("Debezium2Kafka2Delta").master("local")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false").getOrCreate()

  val lakePath = new java.io.File("./tmp/debezium_delta_lake/").getCanonicalPath

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "call-center-db.call-center.calls")
    .option("startingOffsets", "earliest")
//    .option("kafka.group.id", "test")
    .load()

  df.printSchema()

  def list2Tuple7(list: List[String]): (String, String, String, String, String, String, String) = {
    val t = list match {
      case List(a) => (a, "", "", "", "", "", "")
      case List(a, b) => (a, b, "", "", "", "", "")
      case List(a, b, c) => (a, b, c, "", "", "", "")
      case List(a, b, c, d) => (a, b, c, d, "", "", "")
      case List(a, b, c, d, e) => (a, b, c, d, e, "", "")
      case List(a, b, c, d, e, f) => (a, b, c, d, e, f, "")
      case List(a, b, c, d, e, f, g) => (a, b, c, d, e, f, g)
      case _ => ("", "", "", "", "", "", "")
    }
    t
  }

  import spark.implicits._

  val jsonDf = df.selectExpr("CAST(value AS STRING)").as[String]

  val jsonObjectDf = jsonDf.map(json => JsonPathTest.getData(json))

  val query = jsonObjectDf
    .writeStream
    .outputMode("append")
    .format("console")
    .start()

  query.awaitTermination()
}
