package pipeline

import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.collection.JavaConversions._


object GetFileFromKafka {

  def main(args: Array[String]): Unit = {
    println("Inside GetFileFromKafka")
    val kafkaConsumerProperty = new Properties()
    kafkaConsumerProperty.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    kafkaConsumerProperty.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaConsumerProperty.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaConsumerProperty.put(ConsumerConfig.GROUP_ID_CONFIG, "cg01")
    kafkaConsumerProperty.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val topic = "inputFileLists"
    val kafkaConsumer = new KafkaConsumer[String,String](kafkaConsumerProperty)
    kafkaConsumer.subscribe(Collections.singletonList(topic))
    while (true) {
      val fileLists = kafkaConsumer.poll(10000)
      //Read the file paths from kafka topic
      for (filePath <- fileLists) {
        //Launch Spark-Application to process File
        try {
          ApplicationLauncher.launch(filePath.value())
        } catch{
          case e :Throwable => e.printStackTrace
        }
      }
    }
  }

}