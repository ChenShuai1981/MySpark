//import org.apache.spark.sql.{SaveMode, SparkSession}
//import com.uber.hoodie.DataSourceWriteOptions._
//import com.uber.hoodie.config.HoodieWriteConfig.TABLE_NAME
//
//object Hudi2 {
//
//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession.builder
//      .master("local")
//      .appName("Hudi2")
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .enableHiveSupport()
//      .getOrCreate
//
//    import spark.implicits._
//
//    val tableName = "test_data_2"
//
//    val basePath = "file:///Users/chenshuai1/dev/hudi-study/hudi-data/" + tableName
//
//    val jsonData = spark.read.json("file:///Users/chenshuai1/github/MySpark/src/main/resources/insert.json")
//
//    jsonData.write.format("com.uber.hoodie")
//      .option("hoodie.upsert.shuffle.parallelism", "1")
//      .option(HIVE_ASSUME_DATE_PARTITION_OPT_KEY, true)
//      .option(HIVE_PARTITION_FIELDS_OPT_KEY, "part_date")
//      .option(PARTITIONPATH_FIELD_OPT_KEY, "part_date")
//      .option(HIVE_URL_OPT_KEY, "jdbc:hive2://localhost:10000")
//      //    .option(HIVE_USER_OPT_KEY, "hive")
//      //    .option(HIVE_PASS_OPT_KEY, "123")
//      .option(HIVE_DATABASE_OPT_KEY, "test")
//      .option(HIVE_SYNC_ENABLED_OPT_KEY, true)
//      .option(HIVE_TABLE_OPT_KEY, tableName)
//      .option(PRECOMBINE_FIELD_OPT_KEY, "id")
//      .option(RECORDKEY_FIELD_OPT_KEY, "id")
//      .option(TABLE_NAME, tableName)
//      .mode(SaveMode.Append)
//      .save(basePath)
//  }
//}
