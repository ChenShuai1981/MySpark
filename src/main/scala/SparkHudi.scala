//import com.uber.hoodie.DataSourceReadOptions._
//import org.apache.spark.sql.{SaveMode, SparkSession}
//import com.uber.hoodie.config.HoodieWriteConfig._
//
//object SparkHudi {
//
//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession.builder
//      .master("local")
//      .appName("Demo2")
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .enableHiveSupport()
//      .getOrCreate
//
//    import spark.implicits._
//
//    val jsonData = spark.read.json("file:///Users/chenshuai1/github/MySpark/src/main/resources/insert.json")
//
//    val tableName = "test_data"
//    val basePath = "file:///Users/chenshuai1/dev/hudi-study/hudi-data/" + tableName
//
////    jsonData.write.format("com.uber.hoodie")
////      .option("hoodie.upsert.shuffle.parallelism", "1")
////      .option("hoodie.datasource.write.precombine.field", "id")
////      .option("hoodie.datasource.write.recordkey.field", "id")
////      .option(TABLE_NAME, tableName)
////      .mode(SaveMode.Overwrite)
////      .save(basePath)
//
//    val jsonDataDf = spark.read.format("com.uber.hoodie").load(basePath + "/*/*")
//    jsonDataDf.show(false)
//
//    ////  val updateJsonf = spark.read.json("file:///Users/chenshuai1/github/MySpark/src/main/resources/update.json")
//    ////  updateJsonf.write.format("com.uber.hoodie")
//    ////    .option("hoodie.insert.shuffle.parallelism", "2")
//    ////    .option("hoodie.upsert.shuffle.parallelism", "2")
//    ////    .option("hoodie.datasource.write.precombine.field", "id")
//    ////    .option("hoodie.datasource.write.recordkey.field", "id")
//    ////    .option(TABLE_NAME, tableName)
//    ////    .mode(SaveMode.Append)
//    ////    .save(basePath)
//    //
//    //  jsonDataDf.createOrReplaceTempView("hudi_ro_table")
//    //  val commits = spark.sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_ro_table order by commitTime")
//    //    .map(k => k.getString(0)).take(50)
//    //  val beginTime = commits(commits.length - 2) // commit time we are interested in
//    //  println(beginTime)
//    //
//    //  // 增量查询数据
//    //  val incViewDF = spark.read.format("com.uber.hoodie").
//    //    option(VIEW_TYPE_OPT_KEY, VIEW_TYPE_INCREMENTAL_OPT_VAL).
//    //    option(BEGIN_INSTANTTIME_OPT_KEY, beginTime).
//    //    load(basePath)
//    //  incViewDF.show()
//    //  incViewDF.createOrReplaceTempView("hudi_incr_table")
//    //  spark.sql("select `_hoodie_commit_time`, id, name, age, address from  hudi_incr_table where age > 20").show()
//    //
//    //  // 特定时间点查询Permalink
//    //  val beginTime2 = "000"
//    //  // Represents all commits > this time.
//    //  val endTime2 = commits(commits.length - 2)
//    //  // commit time we are interested in
//    //  // 增量查询数据
//    //  val incViewDF2 = spark.read.format("com.uber.hoodie")
//    //    .option(VIEW_TYPE_OPT_KEY, VIEW_TYPE_INCREMENTAL_OPT_VAL)
//    //    .option(BEGIN_INSTANTTIME_OPT_KEY, beginTime2)
//    //    .option(END_INSTANTTIME_OPT_KEY, endTime2)
//    //    .load(basePath)
//    //  incViewDF2.show()
//    //  incViewDF.createOrReplaceTempView("hudi_incr_table2")
//    //  spark.sql("select `_hoodie_commit_time`, id, name, age, address from  hudi_incr_table2 where age < 12").show()
//
//  }
//}
