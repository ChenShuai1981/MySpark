package pipeline

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import java.io.IOException
import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object FileUploaderHDFS {

  val kafkaProducerProperty = new Properties()
  kafkaProducerProperty.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  kafkaProducerProperty.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProducerProperty.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val kafkaProducer = new KafkaProducer[Nothing, String](kafkaProducerProperty)

  def main(args: Array[String]): Unit = {
    try {
      println("Inside FileUploaderHDFS")
      val hadoopConf = new Configuration()
      val hdfs = FileSystem.get(hadoopConf)
      val localPathStr = "<local-input-file-path>"
      val fileList = getListOfFiles(localPathStr)
      val destPath = new Path("<hdfs-destination-path>")
      if (!hdfs.exists(destPath)) {
        hdfs.mkdirs(destPath)
      }

      for (file <- fileList) {
        val localFilePath = new Path("file:" + "//" + file)
        hdfs.copyFromLocalFile(localFilePath, destPath)

        /*Move file to processed folder*/
        Files.move(
          Paths.get(file.toString),
          Paths.get("<Processed-File-Path>"),
          StandardCopyOption.REPLACE_EXISTING)

        produceKafkaMsg(destPath + "/" + file.getName)
        println("File has been uploaded into HDFS " + destPath  )
      }
    } catch {
      case ex: IOException => {
        println("Input /Output Exception")
      }
    }
  }


  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  // This method writes the uploaded file path into Kafka topic
  def produceKafkaMsg(hdfsFile: String):Unit={
    val kafkaProducerRecord = new ProducerRecord("inputFileLists", hdfsFile)
    println(kafkaProducer.send(kafkaProducerRecord))
    println("File has been uploaded into HDFS " + hdfsFile)
  }

}