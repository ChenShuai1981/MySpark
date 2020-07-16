package delta

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

// https://mungingdata.com/delta-lake/merge-update-upserts/
object DeltaMerge extends App {

  val spark = SparkSession.builder().appName("DeltaMerge").master("local").getOrCreate()

  /** write delta table **/
//  val path = new java.io.File("./src/main/resources/event_data/").getCanonicalPath
//  val df = spark
//    .read
//    .option("header", "true")
//    .option("charset", "UTF8")
//    .csv(path)
//
//  val outputPath = new java.io.File("./tmp/event_delta_lake/").getCanonicalPath
//  df
//    .repartition(1)
//    .write
//    .format("delta")
//    .save(outputPath)

  /** read delta table **/
//  val path = new java.io.File("./tmp/event_delta_lake/").getCanonicalPath
//  val df = spark.read.format("delta").load(path)
//  df.show(false)

  /** fix typo clck -> click **/
//  val path = new java.io.File("./tmp/event_delta_lake/").getCanonicalPath
//  val deltaTable = DeltaTable.forPath(spark, path)
//
//  deltaTable.updateExpr(
//    "eventType = 'clck'",
//    Map("eventType" -> "'click'")
//  )
//
//  deltaTable.update(
//    col("eventType") === "clck",
//    Map("eventType" -> lit("click"))
//  )
//
//  spark.read.format("delta").load(path).show(false)

  /** read parquet file directly **/
  val path = new java.io.File("./tmp/event_delta_lake/part-00000-28e15da4-9a70-4c8e-991e-a31d9a9d5316-c000.snappy.parquet").getCanonicalPath
  val df = spark.read.parquet(path)
  df.show(false)
}
