package delta

import delta.DeltaIntro.{df, spark}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{SaveMode, SparkSession}

object DeltaManifest extends App {

  val spark = SparkSession.builder().appName("DeltaManifest").master("local").getOrCreate()

//  val path = new java.io.File("./src/main/resources/person_data/").getCanonicalPath
//  val df = spark
//    .read
//    .option("header", "true")
//    .option("charset", "UTF8")
//    .csv(path)
//
//  df.show()
//
  val outputPath = "hdfs://localhost:9000/person"
//  df
//    .repartition(1)
//    .write
//    .format("delta")
//    .mode(SaveMode.Overwrite)
//    .save(outputPath)

  val deltaTable = DeltaTable.forPath(outputPath)
  deltaTable.toDF.show()
  deltaTable.generate("symlink_format_manifest")
}
