//package stream
//
//import kafka.serializer.StringDecoder
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.{FSDataOutputStream, Path, FileSystem}
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.catalyst.expressions.Second
//import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange, KafkaUtils}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
///**
//  * Created by hunhun on 2016/5/26.
//  */
//object WriteToHdfsWithCheckpoint {
//  def main(args: Array[String]) {
//    if (args.length < 2) {
//      System.err.println(s"""
//                            |Usage: DirectKafkaWordCount <brokers> <topics>
//                            |  <brokers> is a list of one or more Kafka brokers
//                            |  <topics> is a list of one or more kafka topics to consume from
//                            |
//        """.stripMargin)
//      System.exit(1)
//    }
//
//    val Array(brokers, topics) = args
//
//    // Create context with 2 second batch interval
//    val sparkConf = new SparkConf().setAppName("ConsumerKafkaToHdfsWithCheckPoint")
//
//    // Create direct kafka stream with brokers and topics
//    val topicsSet = topics.split(",").toSet
//    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
//      "auto.offset.reset" -> "smallest")
//    val checkpointPath = "hdfs://127.0.0.1:8020/spark_checkpoint"
//    def functionToCreateContext(): StreamingContext = {
//      val ssc = new StreamingContext(sparkConf, Seconds(2))
//      val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
//      // checkpoint 在哪保存？ 这样会不会出现offset保存了但没有成功写入hdfs
//      // checkpoint调用的位置会有影响吗？
//      ssc.checkpoint(checkpointPath)
//
//      messages.map(_._2).foreachRDD(rdd => {
//        rdd.foreachPartition{ partitionOfRecords =>
//          val conf = new Configuration()
//          val fs = FileSystem.get(conf)
//          val path = new Path("/user/admin/scalapath/test" + System.currentTimeMillis())
//          val outputStream : FSDataOutputStream = if (fs.exists(path)){
//            fs.append(path)
//          }else{
//            fs.create(path)
//          }
//          partitionOfRecords.foreach(record => outputStream.write((record + "\n").getBytes("UTF-8")))
//          outputStream.close()
//        }
//      })
//      ssc
//    }
//    // 决定否创建新的Context
//    // 没有checkpoint就创建一个新的StreamingContext即初次启动应用
//    // 如果有checkpoint则checkpoint中记录的信息恢复StreamingContext
//    val context = StreamingContext.getOrCreate(checkpointPath, functionToCreateContext _)
//
//    context.start()
//    context.awaitTermination()
//  }
//}