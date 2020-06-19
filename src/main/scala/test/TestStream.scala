//package test
//
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.FileSystem
//import org.apache.spark.sql.SaveMode
//import org.apache.spark.streaming.kafka010._
//import org.apache.spark.streaming.kafka010.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.{SparkConf, SparkContext}
//
//object TestStream extends App {
//
//  val conf = new SparkConf().setAppName("UserTotal")
//  val sc = new SparkContext(conf)
//  val ssc = new StreamingContext(sc, Seconds(5))
//
//  val kafkaParams = Map[String, String](
//    "metadata.broker.list" -> "node1:9092",
//    "group.id" -> "spark_streaming",
//    "auto.offset.reset" -> "earliest"
//  )
//
//  val topics = Set("user_friend")
//
//  val messages = KafkaUtils.createDirectStream[String, String](
//    ssc,
//    LocationStrategies.PreferConsistent,
//    ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
//  )
//
//  messages.foreachRDD{rdd=> {
//    val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
//    val offsetRanges2 = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//
//    val nowDate =  DateUtils.parseMillisToMinute(System.currentTimeMillis().toString)
//    val nowMDH =  (DateUtils.getMon(nowDate),DateUtils.getDay(nowDate),DateUtils.getHour(nowDate))
//
//    val conf = new Configuration()
//    val fileSystem = FileSystem.get(conf)
//    val hiveTableLocaton =fileSystem.getUri + "/user/hive/warehouse/kafka.db/hn_location/"
//    val isFileAlreadyExsist = fileSystem.exists(new Path(s"${hiveTableLocaton}p_month=${nowMDH._1}/p_day=${nowMDH._2}/p_hour=${nowMDH._3}"))
//
//    spark.createDataFrame(
//      rdd.map(x=>{s"${x.value()}@${x.timestamp()}"})
//        .map(x => LogUtils.parseLog(x)), LogUtils.getSchema()
//    ).coalesce(1).write.format("text").mode(SaveMode.Append) //spark默认的存储格式为parquet
//      .partitionBy("p_month","p_day","p_hour").save(hiveTableLocaton)
//    //刷分区
//    if(!isFileAlreadyExsist) {
//      spark.sql(s"ALTER TABLE kafka.hn_location add if not exists partition(p_month='${nowMDH._1}',p_day='${nowMDH._2}',p_hour='${nowMDH._3}')")
//    }
//    messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges2)
//  }}
//
//}
