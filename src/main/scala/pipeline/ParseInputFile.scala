package pipeline

import org.apache.spark.sql.SparkSession


object ParseInputFile {

  def main(args: Array[String]): Unit = {

    val filePath = args(0)
    val sparkSession = SparkSession.builder().appName("Read Raw XML").enableHiveSupport().getOrCreate()
    val webRawData = sparkSession.read.format("com.databricks.spark.xml").option("rootTag","records").option("rowTag","record").load(filePath)
    webRawData.registerTempTable("tempRawData")
    sparkSession.sql("<Your-HiveQL-To process data from tempRawData table>").registerTempTable("webDataEnrich")
    sparkSession.sql("<Insert-HiveQL-to load data into respective hive table>")
  }


}