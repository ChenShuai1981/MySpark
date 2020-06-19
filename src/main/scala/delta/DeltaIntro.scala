package delta

import org.apache.spark.sql.{SaveMode, SparkSession}

object DeltaIntro extends App {

  val spark = SparkSession.builder()
    .appName("DeltaIntro")
    .master("local")
    .getOrCreate()

  /** write delta table **/
  val path = new java.io.File("./src/main/resources/person_data/").getCanonicalPath
  val df = spark
    .read
    .option("header", "true")
    .option("charset", "UTF8")
    .csv(path)

  val outputPath = new java.io.File("./tmp/person_delta_lake/").getCanonicalPath
  df
    .repartition(1)
    .write
    .format("delta")
    .mode(SaveMode.Overwrite)
    .save(outputPath)
}
