//package com.mongodb.spark
//
//import org.apache.spark.sql.SparkSession
//
//
//object ReadMongo {
//
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder()
//      .master("local")
//      .appName("ReadMongo")
//      .config("spark.mongodb.input.uri", "mongodb://msg_ro:Ja8OmWlou@172.16.81.176:27017/nwd_msg.msg")
//      .getOrCreate()
//
//    // 设置log级别
//    spark.sparkContext.setLogLevel("WARN")
//
//    val df = MongoSpark.load(spark)
//
//    df.createOrReplaceTempView("msg")
//
//    val resDf = spark.sql("select * from msg where memberId = 1020160504719762")
//    resDf.show()
//
//    spark.stop()
//  }
//}
//
