package delta

import org.apache.spark.sql.SparkSession

// https://mungingdata.com/delta-lake/merge-update-upserts/
object DeltaUpsert extends App {

  val spark = SparkSession.builder().appName("DeltaUpsert").master("local").getOrCreate()

  /** write delta table **/
//    val path = new java.io.File("./src/main/resources/upsert_event_data/original_data.csv").getCanonicalPath
//    val df = spark
//      .read
//      .option("header", "true")
//      .option("charset", "UTF8")
//      .csv(path)
//
//    val outputPath = new java.io.File("./tmp/upsert_event_delta_lake/").getCanonicalPath
//    df
//      .repartition(1)
//      .write
//      .format("delta")
//      .save(outputPath)

  /** read delta table **/
//    val path = new java.io.File("./tmp/upsert_event_delta_lake/").getCanonicalPath
//    val df = spark.read.format("delta").load(path)
//    df.show(false)

  /** upsert **/
//    val updatesPath = new java.io.File("./src/main/resources/upsert_event_data/mom_friendly_data.csv").getCanonicalPath
//    val updatesDF = spark
//      .read
//      .option("header", "true")
//      .option("charset", "UTF8")
//      .csv(updatesPath)
//
//    val path = new java.io.File("./tmp/upsert_event_delta_lake/").getCanonicalPath
//
//    import io.delta.tables._
//
//    DeltaTable.forPath(spark, path)
//      .as("events")
//      .merge(
//        updatesDF.as("updates"),
//        "events.eventId = updates.eventId"
//      )
//      .whenMatched
//      .updateExpr(
//        Map("data" -> "updates.data")
//      )
//      .whenNotMatched
//      .insertExpr(
//        Map(
//          "date" -> "updates.date",
//          "eventId" -> "updates.eventId",
//          "data" -> "updates.data")
//      )
//      .execute()


  //  UpsertEventProcessor.displayEventParquetFile("part-00000-2f0e03ac-52de-4667-a4b9-32239a0349c1-c000")

  //  +----+-------+----+
  //  |date|eventId|data|
  //  +----+-------+----+
  //  +----+-------+----+

  //  UpsertEventProcessor.displayEventParquetFile("part-00026-a19752c0-d1b7-43c8-ae6d-fe822c4d2908-c000")

  //  +----------+-------+----------------+
  //  |date      |eventId|data            |
  //  +----------+-------+----------------+
  //  |2019-02-05|8      |bond with nephew|
  //  +----------+-------+----------------+

  //  UpsertEventProcessor.displayEventParquetFile("part-00139-3e4c8a25-3382-4932-9972-4a34b4048d32-c000")

  //  +----------+-------+---------------------+
  //  |date      |eventId|data                 |
  //  +----------+-------+---------------------+
  //  |2019-04-24|9      |speak at spark summit|
  //  +----------+-------+---------------------+

  //  UpsertEventProcessor.displayEventParquetFile("part-00166-38e37448-7d09-4414-8ff6-cd17f8286514-c000")

  //  +----------+-------+---------+
  //  |date      |eventId|data     |
  //  +----------+-------+---------+
  //  |2019-01-01|4      |set goals|
  //  +----------+-------+---------+

  //  UpsertEventProcessor.displayEventParquetFile("part-00178-6d0ed3c4-fa83-403e-a5cf-2d30cd8fda12-c000")

  //  +----------+-------+--------------------+
  //  |date      |eventId|data                |
  //  +----------+-------+--------------------+
  //  |2019-08-10|66     |think about my mommy|
  //  +----------+-------+--------------------+

  /** History and Time Travel **/

  val lakePath = new java.io.File("./tmp/upsert_event_delta_lake/").getCanonicalPath

//  import io.delta.tables._
//
//  val deltaTable = DeltaTable.forPath(spark, lakePath)
//  val fullHistoryDF = deltaTable.history()
//  fullHistoryDF.show()

  //  +-------+-------------------+------+--------+---------+--------------------+----+--------+---------+-----------+--------------+-------------+--------------------+
  //  |version|          timestamp|userId|userName|operation| operationParameters| job|notebook|clusterId|readVersion|isolationLevel|isBlindAppend|    operationMetrics|
  //  +-------+-------------------+------+--------+---------+--------------------+----+--------+---------+-----------+--------------+-------------+--------------------+
  //  |      1|2020-05-17 16:18:19|  null|    null|    MERGE|[predicate -> (ev...|null|    null|     null|          0|          null|        false|[numTargetRowsCop...|
  //  |      0|2020-05-17 16:14:27|  null|    null|    WRITE|[mode -> ErrorIfE...|null|    null|     null|       null|          null|         true|[numFiles -> 1, n...|
  //  +-------+-------------------+------+--------+---------+--------------------+----+--------+---------+-----------+--------------+-------------+--------------------+

  // 根据时间戳
//  spark
//    .read
//    .format("delta")
//    .option("timestampAsOf", "2020-07-16 15:03:47")
//    .load(lakePath)
//    .show()

  // 根据版本号
  spark
    .read
    .format("delta")
    .option("versionAsOf", 1)
    .load(lakePath)
    .show()
}
