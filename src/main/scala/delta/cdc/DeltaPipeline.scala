package delta.cdc

import io.delta.tables.DeltaTable
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, when}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object DeltaPipeline extends App {

  val spark = SparkSession.builder()
    .appName("DeltaPipeline")
    .master("local[1]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  spark.sql("set spark.sql.shuffle.partitions = 1")
  spark.sql("set spark.databricks.delta.snapshotPartitions = 1")
  spark.sql("DROP TABLE IF EXISTS voter")

  rm(VOTER_INPUT_PATH)
  mkdir(VOTER_INPUT_PATH)

  rm(VOTER_TABLE_PATH)

  spark.sql("DROP TABLE IF EXISTS voter")

  spark.sql(s"""
               |CREATE TABLE voter
               |      (
               |        db STRING NOT NULL,
               |        table STRING NOT NULL,
               |        at TIMESTAMP NOT NULL,
               |
               |        id STRING NOT NULL,
               |        name STRING NOT NULL,
               |        address STRING NOT NULL
               |      )
               |      USING DELTA
               |      PARTITIONED BY (id)
               |      LOCATION '${VOTER_TABLE_PATH}'
               |""".stripMargin)

  spark.sql("select * from voter").show(false)

  val voterTable = DeltaTable.forPath(spark, VOTER_TABLE_PATH)

  // For each batch, update the table with Delta capabilities (ACID)
  def updateVoterTable(microBatchDF: Dataset[Row], batchId: Long): Unit = {
    // Take the latest event per id
    val latestChangesDF = microBatchDF.withColumn("row_num", row_number().over(
      Window.partitionBy("db", "table", "id").orderBy(col("at").desc)
    )).where("row_num == 1")

    voterTable.alias("t").merge(latestChangesDF.alias("s"), "t.db = s.db and t.table = s.table and t.id = s.id")
      .whenMatched(condition = "s.op = 'd'").delete()
      .whenMatched(condition = "s.op != 'd'").updateExpr(MATCH_TABLE_MAP)
      .whenNotMatched(condition = "s.op != 'd'").insertExpr(MATCH_TABLE_MAP)
      .execute()
  }

  val query = spark.readStream.schema(debeziumEventSchema).json(VOTER_INPUT_PATH)
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
    .foreachBatch(updateVoterTable _)
    .outputMode("update")
    .start()

  query.awaitTermination()

}
