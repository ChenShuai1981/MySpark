import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupTopN {

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
    val tupleRDD: RDD[(String, Int)] = lines.map(line => {
      (line.split(",")(0), line.split(",")(1).toInt)
    })
    //分组
    val groutRDD: RDD[(String, Iterable[Int])] = tupleRDD.groupByKey()
    //针对分组对value排序,返回Tuple2
    val groupSort: RDD[(String, List[Int])] = groutRDD.map(grouped => {
      (grouped._1, grouped._2.toList.sortWith(_ > _).take(3)) //升序，取Top3
    })
    //遍历输出
    groupSort.sortByKey().collect().foreach(pair => {
      println(pair._1 + ":")
      pair._2.foreach(s => println(s + "\t"))
    })

    sc.stop()
  }
}