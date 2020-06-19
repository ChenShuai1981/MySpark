package delta

import org.apache.spark.sql.{SaveMode, SparkSession}

// https://mungingdata.com/delta-lake/compact-small-files/
object DeltaCompact extends App {

  val spark = SparkSession.builder().appName("DeltaCompact").master("local")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false").getOrCreate()

  val lakePath = new java.io.File("./tmp/compact_delta_lake/").getCanonicalPath
//  val data = spark.range(0, 1000)
//  data.repartition(1000).write.format("delta").save(lakePath)

//  val df = spark
//    .read
//    .format("delta")
//    .load(lakePath)
//
//  df
//    .repartition(10)
//    .write
//    .format("delta")
//    .mode(SaveMode.Overwrite)
//    .save(lakePath)
//
//  spark.read.format("delta").load(lakePath).show(false)

//  import io.delta.tables._
//  val deltaTable = DeltaTable.forPath(spark, lakePath)
//  deltaTable.vacuum(0.000001)
}
