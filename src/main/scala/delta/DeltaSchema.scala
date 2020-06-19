package delta

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType}
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._

// https://mungingdata.com/delta-lake/schema-enforcement-evolution-mergeschema-overwriteschema/
object DeltaSchema extends App {

  val spark = SparkSession.builder().appName("DeltaCompact").master("local")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .getOrCreate()


//  val df = spark.createDF(
//    List(
//      (1, 2),
//      (3, 4)
//    ), List(
//      ("num1", IntegerType, true),
//      ("num2", IntegerType, true)
//    )
//  )
//
  val parquetPath = new java.io.File("./tmp/parquet_schema/").getCanonicalPath
//
//  df.write.parquet(parquetPath)
//
//  spark.read.parquet(parquetPath).show()
//
//  val df2 = spark.createDF(
//    List(
//      88,
//      99
//    ), List(
//      ("num2", IntegerType, true)
//    )
//  )
//
//  df2.write.mode("append").parquet(parquetPath)

//  val df = spark.createDF(
//    List(
//      (1, 2),
//      (3, 4)
//    ), List(
//      ("num1", IntegerType, true),
//      ("num2", IntegerType, true)
//    )
//  )
//
  val deltaPath = new java.io.File("./tmp/schema_example/").getCanonicalPath
//
//  df.write.format("delta").save(deltaPath)

//  val df2 = spark.createDF(
//    List(
//      88,
//      99
//    ), List(
//      ("num1", IntegerType, true)
//    )
//  )
//
//  df2.write.format("delta").mode("append").save(deltaPath)

//  val df3 = spark.createDF(
//    List(
//      101,
//      102
//    ), List(
//      ("num2", IntegerType, true)
//    )
//  )
//
//  df3.write.format("delta").mode("append").save(deltaPath)

//  val df4 = spark.createDF(
//    List(
//      (7, 7, 7),
//      (8, 8, 8)
//    ), List(
//      ("num1", IntegerType, true),
//      ("num2", IntegerType, true),
//      ("num3", IntegerType, true)
//    )
//  )
//
//  df4.write.format("delta").mode("append")
//    .option("mergeSchema", "true").save(deltaPath)

  val df5 = spark.createDF(
    List(
      ("nice", "person"),
      ("like", "madrid")
    ), List(
      ("word1", StringType, true),
      ("word2", StringType, true)
    )
  )

  df5
    .write
    .format("delta")
//    .mode("append")
    .mode("overwrite")
//    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .save(deltaPath)

  spark.read.format("delta").load(deltaPath).show()
}
