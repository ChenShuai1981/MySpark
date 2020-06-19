import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

object GroupTopN2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName}")
    conf.setMaster("local")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    /**
      * 计算topN
      */
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("/Users/chenshuai1/github/MySpark/src/main/resources/groupTopN.txt")
    //拆分为Tuple2
    val tupleRDD: RDD[((String, Int), Int)] = lines.map(line => {
      val items = line.split(",")
      // 因为repartitionAndSortWithinPartitions算子只能升序排列，所以为了取得topN需要跟最大数相减(最大就变成最小)处理一下
      ((items(0), Int.MaxValue - items(1).toInt), 1)
    })

    val sorted = tupleRDD.repartitionAndSortWithinPartitions(new HashPartitioner(lines.getNumPartitions))
    sorted.foreach(println)

    val filterForTargetIndex = sorted.mapPartitions(iter => {
      var currentLang = ""
      var runningTotal = 0
      iter.filter({
        case ((lang, score), _) => {
          if (!lang.equals(currentLang)) {
            currentLang = lang //reset to the new column index
            runningTotal = 1
          } else {
            runningTotal += 1
          }
        }
        runningTotal <= 3
      })
    }.map(s => (s._1._1, Int.MaxValue - s._1._2)), preservesPartitioning = true) // 此处需要将最小值还原成最大值

    val map = groupSorted(filterForTargetIndex.collect())

    map.foreach(t => {
      println(t._1 + " -> " + t._2.mkString(","))
    })

    sc.stop()
  }

  private def groupSorted(it: Array[(String, Int)]): Map[String, Iterable[Int]] = {
    val res = List[(String, ArrayBuffer[Int])]()
    it.foldLeft(res)((list, next) => list match {
      case Nil =>
        val (firstKey, value) = next
        List((firstKey, ArrayBuffer(value)))
      case head :: rest =>
        val (curKey, valueBuf) = head
        val (firstKey, value) = next
        if (!firstKey.equals(curKey)) {
          (firstKey, ArrayBuffer(value)) :: list
        } else {
          valueBuf.append(value)
          list
        }
    }).map { case (key, buf) => (key, buf.toIterable) }.toMap
  }

}
