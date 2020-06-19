package com.iteblog.mysql

import org.apache.spark.sql.SparkSession

object TestCase extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("TestCase")
    .getOrCreate()

  val df = spark.read.format("com.iteblog.mysql")
    .option("url", "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf8")
    .option("dbtable", "student")
    .option("user", "root")
    .option("password", "root")
    .option("driver", "com.mysql.jdbc.Driver")
    .load().filter("id > 140")

  df.printSchema()
  df.show()

}
