import com.mongodb.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.bson._


object QueryMongo extends App {

  val spark = SparkSession.builder
    .appName(this.getClass.getName().stripSuffix("$"))
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  val inputUri="mongodb://nwd_msg_ro:Ker9bn5sgM@172.16.81.91:27017/nwd_msg.msg_201808"
  val df = spark.read.format("com.mongodb.spark.sql").options(
    Map("spark.mongodb.input.uri" -> inputUri,
      "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
      "spark.mongodb.input.partitionerOptions.partitionKey"  -> "_id",
      "spark.mongodb.input.partitionerOptions.partitionSizeMB"-> "32"))
    .load()
//  val currentTimestamp = System.currentTimeMillis()
//  val originDf = df.filter(df("updateTime") < currentTimestamp && df("updateTime") >= currentTimestamp - 1440 * 60 * 1000)
//    .select("_id", "content", "imgTotalCount").toDF("id", "content", "imgnum")

  df.createOrReplaceTempView("msg")
  spark.sql("select * from msg limit 1").show(3)
}
