package delta

import org.apache.spark.sql.{SaveMode, SparkSession}

// https://mungingdata.com/delta-lake/vacuum-command/
object DeltaVacuum extends App {

  val spark = SparkSession.builder()
    .appName("DeltaVacuum")
    .master("local")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .getOrCreate()

  /** write delta table **/
//  val path = new java.io.File("./src/main/resources/person_data/people1.csv").getCanonicalPath
//  val df = spark
//    .read
//    .option("header", "true")
//    .option("charset", "UTF8")
//    .csv(path)
//
//  val outputPath = new java.io.File("./tmp/vacuum_example/").getCanonicalPath
//  df
//    .repartition(1)
//    .write
//    .format("delta")
//    .save(outputPath)

  /** read delta table **/
//  val path = new java.io.File("./tmp/vacuum_example/").getCanonicalPath
//  val df = spark.read.format("delta").load(path)
//  df.show()

  /** overwrite with another csv **/
//  val path = new java.io.File("./src/main/resources/person_data/people2.csv").getCanonicalPath
//  val df = spark
//    .read
//    .option("header", "true")
//    .option("charset", "UTF8")
//    .csv(path)
//
//  val outputPath = new java.io.File("./tmp/vacuum_example/").getCanonicalPath
//  df
//    .repartition(1)
//    .write
//    .format("delta")
//    .mode(SaveMode.Overwrite)
//    .save(outputPath)

  /** read version 0 data **/
//  val path = new java.io.File("./tmp/vacuum_example/").getCanonicalPath
//  val df = spark.read.format("delta").option("versionAsOf", 0).load(path)
//  df.show()

    /** vacuum **/
//    val path = new java.io.File("./tmp/vacuum_example/").getCanonicalPath
//    import io.delta.tables._
//    val deltaTable = DeltaTable.forPath(spark, path)
//    deltaTable.vacuum(0.000001)

}
