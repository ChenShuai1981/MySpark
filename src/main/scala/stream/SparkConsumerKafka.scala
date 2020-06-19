//package stream
//
//import kafka.serializer.StringDecoder
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.{FSDataOutputStream, Path, FileSystem}
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//import scala.collection.mutable.ListBuffer
//import scala.util.Random
//
///**
//  * Created by hunhun on 2016/5/23.
//  */
//object SparkConsumerKafka {
//  def main(args: Array[String]) {
//    if (args.length < 4) {
//      System.err.println( s"""
//                             |Usage: DirectKafkaWordCount <brokers> <topics> <groupid>
//                             |  <brokers> is a list of one or more Kafka brokers
//                             |  <topics> is a list of one or more kafka topics to consume from
//                             |  <groupid> is a consume group
//                             |  <hdfspath> is a HDFS Path, like /user/admin/scalapath
//                             |
//        """.stripMargin)
//      System.exit(1)
//    }
//
//    val Array(brokers, topics, groupId, hdfsPath) = args
//    val sparkConf = new SparkConf().setAppName("SparkConsumerKafka")
//    // spark.streaming.kafka.maxRatePerPartition 限速
//    val ssc = new StreamingContext(sparkConf, Seconds(60))
//
//    // Create direct kafka stream with brokers and topics
//    val topicsSet = topics.split(",").toSet
//    val kafkaParams = Map[String, String](
//      "metadata.broker.list" -> brokers,
//      "group.id" -> groupId,
//      "auto.offset.reset" -> "smallest"
//    )
//
//    val km = new KafkaManager(kafkaParams)
//
//    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
//    // foreachRDD是DStream的output操作
//    messages.foreachRDD( rdd => {
//      if (!rdd.isEmpty()){
//        rdd.foreachPartition { partitionOfRecords =>
//          // 得到HDFS的操作client
//          // 此代码必须放在worker中创建，如果在driver中创建，则将会被序列化到worker中
//          // 在worker中操作时会报错。
//          val conf = new Configuration()
//          val fs = FileSystem.get(conf)
//
//          var messageList = new ListBuffer[String]
//          var topic = ""
//          // 此处代码只为得到topic
//          // 之所以将message放入messageList中是因为partitionOfRecords变为list之后
//          // 拿到一个record的topic之后，partitionOfRecords中的内容将消失，具体原因不知道
//          // 代码如下：
//          // val topic = partitionOfRecords.toList(0)._3
//          // 然后写入hdfs时调用
//          // partitionOfRecords.foreach(record => outputStream.write((record._2 + "\n").getBytes("UTF-8")))
//          // 此时写入hdfs的内容为null，不知道为什么为null
//          // 所以只好在得到topic的同时把message先存入messageList
//          partitionOfRecords.foreach(record => {
//            messageList += record._2
//            if (topic == ""){
//              topic = record._3
//            }
//          })
//
//          if (topic != ""){
//            // 拼出各个topic message的文件地址
//            val path = new Path(hdfsPath + "/" + topic + "/" + Random.nextInt(100) + topic + System.currentTimeMillis())
//            // 创建一个HDFS outputStream流
//            val outputStream = if (fs.exists(path)){
//              fs.append(path)
//            } else{
//              fs.create(path)
//            }
//            // 将message逐条写入
//            messageList.foreach(message => outputStream.write((message + "\n").getBytes("UTF-8")))
//            outputStream.close()
//          }
//
//        }
//        // 更新zk上的offset
//        km.updateZKOffsets(rdd)
//      }
//    })
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}