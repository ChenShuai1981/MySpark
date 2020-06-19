import org.apache.spark.{SparkConf, SparkContext}

object TestSample extends App {

  val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[1]")
  val sc = new SparkContext(sparkConf)

  val data = Array("A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A",
    "A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A",
    "A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A",
    "B","B","B","B","B","B","B","B","C","D","E","F","G")
  val rdd= sc.parallelize(data)

  val tupleRdd: Array[(Int, String)] =rdd.map(line=>(line,1)) //变成 (word,1) 的形式
    .sample(false,0.4) //采样，取40%做样本
    .reduceByKey((x,y)=>x+y)  //单词统计 结果类似 (A,8),(B,18)
    .map(line=>(line._2,line._1)) //==>交换顺序(8，A) ,(18,B) 为了便于后面把单词次数多的，排序筛选出来
    .sortByKey(ascending = false)
//    .sortBy(line=>line._1,false,2) //排序,按照单词频次倒排
    .take(3) //取出来单词数前三的 ，这些可能就是脏key 后者热点key

  for(ele <- tupleRdd){
    println(ele._2+"===的个数为======"+ele._1)
  }

}
