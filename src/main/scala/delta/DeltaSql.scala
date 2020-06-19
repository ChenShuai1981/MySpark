package delta

import org.apache.spark.sql.SparkSession

object DeltaSql extends App {

  val spark = SparkSession.builder().appName("DeltaSql").master("local").getOrCreate()

//  val path = new java.io.File("./src/main/resources/person_data").getCanonicalPath
//  val df = spark
//    .read
//    .option("header", "true")
//    .option("charset", "UTF8")
//    .csv(path)

  val lakePath = new java.io.File("./tmp/sql_delta_lake/").getCanonicalPath

//  df
//    .repartition(1)
//    .write
//    .format("delta")
//    .save(lakePath)

  val df = spark.read.format("delta").load(lakePath)

  df.createOrReplaceTempView("person")

  spark.sql("insert into person (first_name, last_name, country) values ('casel', 'chen', 'china')")

  spark.sql("select * from person").show()

}
