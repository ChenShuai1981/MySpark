package delta

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StringType

// https://mungingdata.com/delta-lake/updating-partitions-with-replacewhere/
object DeltaReplaceWhere extends App {

  val spark = SparkSession.builder().appName("DeltaCompact").master("local")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false").getOrCreate()

  val path = new java.io.File("./src/main/resources/replace_data/").getCanonicalPath

  val df = spark
    .read
    .option("header", "true")
    .option("charset", "UTF8")
    .csv(path)
    .withColumn("continent", lit(null).cast(StringType))

  val deltaPath = new java.io.File("./tmp/country_partitioned_lake/").getCanonicalPath

  df
    .repartition(col("country"))
    .write
    .partitionBy("country")
    .format("delta")
    .mode("overwrite")
    .save(deltaPath)

  def withContinent()(df: DataFrame): DataFrame = {
    df.withColumn(
      "continent",
      when(col("country") === "Russia", "Europe")
        .when(col("country") === "China", "Asia")
        .when(col("country") === "Argentina", "South America")
    )
  }

  spark.read.format("delta").load(deltaPath)
    .where(col("country") === "China")
    .transform(withContinent())
    .write
    .format("delta")
    .option("replaceWhere", "country = 'China'")
    .mode("overwrite")
    .save(deltaPath)

  spark
    .read
    .format("delta")
    .load(deltaPath)
    .show(false)

}
