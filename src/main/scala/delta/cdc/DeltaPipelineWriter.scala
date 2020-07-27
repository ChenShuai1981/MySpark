package delta.cdc

import org.apache.spark.sql.SparkSession

import scala.io.Source

object DeltaPipelineWriter extends App {

  val spark = SparkSession.builder()
    .appName("DeltaPipelineWriter")
    .master("local[1]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

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

  // 写入初始数据
  val initCdcData = Source.fromFile(INIT_DATA)
  val initEvents = initCdcData.getLines.toList
  writeVoterEvents(initEvents)

  Thread.sleep(5000)
  println("写入初始数据后查询结果")
  spark.sql("select * from voter").show(false)

  // 写入更新数据
  val updateCdcData = Source.fromFile(UPDATE_DATA)
  val updateEvents = updateCdcData.getLines.toList
  writeVoterEvents(updateEvents)

  Thread.sleep(5000)
  println("写入更新数据后查询结果")
  spark.sql("select * from voter").show(false)

}
