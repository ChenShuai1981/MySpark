package delta

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import scala.util.Random
import scala.util.parsing.json.JSON

object DeltaPipeline extends App {

  val spark = SparkSession.builder()
    .appName("DeltaPipeline")
    .master("local")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  spark.sql("set spark.sql.shuffle.partitions = 1")
  spark.sql("set spark.databricks.delta.snapshotPartitions = 1")
  spark.sql("DROP TABLE IF EXISTS voter")

  val VOTER_INPUT_PATH = "hdfs://localhost:9000/dbfs/voter/bronze/"
  val VOTER_TABLE_PATH = "hdfs://localhost:9000/dbfs/voter/silver/"

//  val trueSql = s"""
//                   |CREATE TABLE voter
//                   |      (
//                   |        db STRING NOT NULL,
//                   |        table STRING NOT NULL,
//                   |        at TIMESTAMP NOT NULL,
//                   |
//                   |        id STRING NOT NULL,
//                   |        name STRING NOT NULL,
//                   |        address STRING NOT NULL
//                   |      )
//                   |      USING DELTA
//                   |      PARTITIONED BY (id)
//                   |      LOCATION '${VOTER_TABLE_PATH}'
//                   |""".stripMargin
//
//  println(trueSql)
//
//  spark.sql(trueSql)
//
//  spark.sql("select * from voter").show(false)

  val originTableSchema = new StructType()
    .add(StructField("id", StringType, false))
    .add(StructField("name", StringType, false))
    .add(StructField("address", StringType, false))

  val debeziumSourceSchema = new StructType()
    .add(StructField("db", StringType, false))
    .add(StructField("table", StringType, false))

  val debeziumEventSchema = new StructType(Array(
    StructField("payload", new StructType()
      .add(StructField("op", StringType, false))
      .add(StructField("ts_ms", LongType, false))
      .add(StructField("source", debeziumSourceSchema, false))
      .add(StructField("before", originTableSchema, true))
      .add(StructField("after", originTableSchema, true))
    , false)))

  val MATCH_TABLE_MAP = Map(
    "db" -> "s.db",
    "table" -> "s.table",
    "at" -> "s.at",
    "id" -> "s.id",
    "name" -> "s.name",
    "address" -> "s.address"
  )

  // For each batch, update the table with Delta capabilities (ACID)
  def updateVoterTable(microBatchDF: Dataset[Row], batchId: Long): Unit = {
    // Take the latest event per id
    val latestChangesDF = microBatchDF.withColumn("row_num", row_number().over(
      Window.partitionBy("db", "table", "id").orderBy(col("at").desc)
    )).where("row_num == 1")

    val voterTable = DeltaTable.forPath(spark, VOTER_TABLE_PATH)

    voterTable.alias("t").merge(latestChangesDF.alias("s"), "t.db = s.db and t.table = s.table and t.id = s.id")
      .whenMatched(condition = "s.op = 'd'").delete()
      .whenMatched(condition = "s.op != 'd'").updateExpr(MATCH_TABLE_MAP)
      .whenNotMatched(condition = "s.op != 'd'").insertExpr(MATCH_TABLE_MAP)
      .execute()
  }

  val stream = spark.readStream.schema(debeziumEventSchema).json(VOTER_INPUT_PATH)
  .select(
    col("payload.op"),
    col("payload.ts_ms"),
    col("payload.source.db"),
    col("payload.source.table"),
    when(col("payload.op") === "d", col("payload.before.id")).otherwise(col("payload.after.id")).alias("id"),
    when(col("payload.op") === "d", col("payload.before.name")).otherwise(col("payload.after.name")).alias("name"),
    when(col("payload.op") === "d", col("payload.before.address")).otherwise(col("payload.after.address")).alias("address"))
  .withColumn("at", col("ts_ms") / 1000)
  .writeStream
    .format("delta")
    .foreachBatch((df: Dataset[Row], batchId: Long) => updateVoterTable(df, batchId))
    .outputMode("update")
    .start()

//  def writeVoterEvents(events: String) {
//    val random = new Random()
//    val file = VOTER_INPUT_PATH + "/" + random.nextInt(1000000).toString + ".json"
////    with open("/dbfs" + file, 'w') as f:
////    for event in events:
////      f.write
////    (json.dumps(event) + os.linesep)
//
//    JSON.pa
//    FileUtils.writeLines(file, )
//    file
//  }

//  writeVoterEvents(
//    """
//      |[
//      |    {
//      |      "payload": {
//      |      "op": "c",
//      |      "ts_ms": 1486500510000,
//      |      "source": { "db": "some-db", "table": "some-table" },
//      |      "before": None,
//      |      "after": { "id": "1", "name": "Yulia", "address": "Tel Aviv" }
//      |    }
//      |    },
//      |    {
//      |      "payload": {
//      |      "op": "c",
//      |      "ts_ms": 1486500510000,
//      |      "source": { "db": "some-db", "table": "some-table" },
//      |      "before": None,
//      |      "after": { "id": "2", "name": "Dany", "address": "Jerusalem" }
//      |    }
//      |    },
//      |    {
//      |      "payload": {
//      |      "op": "u",
//      |      "ts_ms": 1486500530000,
//      |      "source": { "db": "some-db", "table": "some-table" },
//      |      "before": { "id": "1", "name": "Yulia", "address": "Haifa" },
//      |      "after": { "id": "1", "name": "Yulia", "address": "Tel Aviv" }
//      |    }
//      |    },
//      |    {
//      |      "payload": {
//      |      "op": "c",
//      |      "ts_ms": 1486500530000,
//      |      "source": { "db": "some-db", "table": "some-table" },
//      |      "before": None,
//      |      "after": { "id": "3", "name": "Avi", "address": "Eilat" }
//      |    }
//      |    }
//      |  ]
//      |""".stripMargin)
}
