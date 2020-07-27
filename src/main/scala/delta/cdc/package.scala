package delta

import java.net.URI
import java.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

package object cdc {

  val VOTER_INPUT_PATH = "hdfs://localhost:9000/dbfs/voter/bronze/"
  val VOTER_TABLE_PATH = "hdfs://localhost:9000/dbfs/voter/silver/"

  val INIT_DATA = "/Users/chenshuai1/github/MySpark/src/main/resources/cdc/init-cdc-data.json"
  val UPDATE_DATA = "/Users/chenshuai1/github/MySpark/src/main/resources/cdc/update-cdc-data.json"

  val originTableSchema = new StructType()
    .add(StructField("id", StringType, false))
    .add(StructField("name", StringType, false))
    .add(StructField("address", StringType, false))

  val debeziumSourceSchema = new StructType()
    .add(StructField("db", StringType, false))
    .add(StructField("table", StringType, false))

  val debeziumEventSchema = new StructType(Array(
    StructField("payload", new StructType()
      .add(StructField("op", StringType, false))
      .add(StructField("ts_ms", LongType, false))
      .add(StructField("source", debeziumSourceSchema, false))
      .add(StructField("before", originTableSchema, true))
      .add(StructField("after", originTableSchema, true))
      , false)))

  val MATCH_TABLE_MAP = Map(
    "db" -> "s.db",
    "table" -> "s.table",
    "at" -> "s.at",
    "id" -> "s.id",
    "name" -> "s.name",
    "address" -> "s.address"
  )

  def writeVoterEvents(events: List[String]) = {
    val random = new Random()
    val realUrl = VOTER_INPUT_PATH + "/" + random.nextInt(1000000).toString + ".json"
    val config = new Configuration()
    val hdfs = FileSystem.get(URI.create(realUrl), config)
    val os = hdfs.create(new Path(realUrl))
    os.write(events.mkString("\n").getBytes("UTF-8"))
    os.close()
    hdfs.close()
  }


  def mkdir(realUrl: String): Unit = {
    val config = new Configuration()
    val fs = FileSystem.get(URI.create(realUrl), config)
    if (!fs.exists(new Path(realUrl))) {
      fs.mkdirs(new Path(realUrl))
    }
    fs.close()
  }

  def rm(realUrl: String): Unit = {
    val config = new Configuration()
    val fs = FileSystem.get(URI.create(realUrl), config)
    if (fs.exists(new Path(realUrl))) {
      fs.delete(new Path(realUrl), true)
    }
    fs.close()
  }

}
