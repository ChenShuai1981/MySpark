import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

object Demo extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[1]")
  val sc = new SparkContext(sparkConf)
//  var rdd = sc.parallelize(1 to 1000).cache()
  // sum(2~1001)
  // sum(3~1002)
  // ...
  // sum(101~1100)
//  for (i <- 1 to 100) {
//    rdd = rdd.map(k => k+1).cache()
////    rdd.foreach(println)
//    val sum = rdd.reduce((num1, num2) => {
////      println(num1 + "," + num2)
//      num1 + num2
//    })
//    println(sum)
//  }

//  val a = Array(1,2,3,4,4,4,4)
//  val b = Array(3, 4)
//  val rddA = sc.parallelize(a)
//  val rddB = sc.parallelize(b)
//  val intersection = rddA.intersection(rddB)
//  intersection.foreach(println)
//  val subtraction = rddA.subtract(rddB)
//  subtraction.foreach(println)
//  val union = intersection.union(subtraction)
//  union.foreach(println)

  val spark = SparkSession.builder.config(sparkConf).getOrCreate
  import spark.implicits._

  val nameSizeRDD: RDD[(String, Double)] = sc.parallelize(Seq(("Happy", 1.0), ("Sad", 0.9), ("Happy", 1.5), ("Coffee", 3.0)))
  val df1 = nameSizeRDD.toDF("name", "size")

  val nameZipRDD = sc.parallelize(Seq(("Happy", "94110"), ("Happy", "94103"), ("Coffee", "10504"), ("Tea", "07012")))
  val df2 = nameZipRDD.toDF("name", "zip")

  // inner join
  println("inner join")
  df1.join(df2, df1("name") === df2("name"), "inner").show()

  // Left outer join
  println("Left outer join")
  df1.join(df2, df1("name") === df2("name"), "left_outer").show()

  // Right outer join
  println("Right outer join")
  df1.join(df2, df1("name") === df2("name"), "right_outer").show()

  // Full outer join
  println("Full outer join")
  df1.join(df2, df1("name") === df2("name"), "full_outer").show()

  // Left semi join
  println("Left semi join")
  df1.join(df2, df1("name") === df2("name"), "left_semi").show()

  // self join
  println("self join")
  df1.as("a").join(df1.as("b")).where($"a.name" === $"b.name").show()


//  def manualBroadCastHashJoin[K : Ordering : ClassTag, V1 : ClassTag, V2 : ClassTag]
//                             (bigRDD : RDD[(K, V1)], smallRDD : RDD[(K, V2)])= {
//    val smallRDDLocal: collection.Map[K, V2] = smallRDD.collectAsMap()
//    bigRDD.sparkContext.broadcast(smallRDDLocal)
//    bigRDD.mapPartitions(iter => { iter.flatMap {
//      case (k, v1) => smallRDDLocal.get(k) match {
//        case None => Seq.empty[(K, (V1, V2))]
//        case Some(v2) => Seq((k, (v1, v2)))
//      }
//    }}, preservesPartitioning = true)
//  }
}
