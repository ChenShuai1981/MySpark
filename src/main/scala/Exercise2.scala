import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * 题目如下：
  *
  * 1. 读取文件的数据test.txt
  *
  * 2. 一共有多少个小于20岁的人参加考试？
  *
  * 3. 一共有多少个等于20岁的人参加考试？
  *
  * 4. 一共有多少个大于20岁的人参加考试？
  *
  * 5. 一共有多个男生参加考试？
  *
  * 6. 一共有多少个女生参加考试？
  *
  * 7. 12班有多少人参加考试？
  *
  * 8. 13班有多少人参加考试？
  *
  * 9. 语文科目的平均成绩是多少？
  *
  * 10. 数学科目的平均成绩是多少？
  *
  * 11. 英语科目的平均成绩是多少？
  *
  * 12. 每个人平均成绩是多少？
  *
  * 13. 12班平均成绩是多少？
  *
  * 14. 12班男生平均总成绩是多少？
  *
  * 15. 12班女生平均总成绩是多少？
  *
  * 16. 13班平均成绩是多少？
  *
  * 17. 13班男生平均总成绩是多少？
  *
  * 18. 13班女生平均总成绩是多少？
  *
  * 19. 全校语文成绩最高分是多少？
  *
  * 20. 12班语文成绩最低分是多少？
  *
  * 21. 13班数学最高成绩是多少？
  *
  * 22. 总成绩大于150分的12班的女生有几个？
  *
  * 23. 总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少？
  *
  */
object Exercise2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local")
      .appName("Exercise")
      .getOrCreate

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val rawRdd = spark.sparkContext.textFile("/Users/chenshuai/github/MySpark/src/main/resources/test.txt")
    val ds: Dataset[StudentScore] = rawRdd.map(line => {
      val items = line.split(" ")
      new StudentScore(items(0).toInt, items(1), items(2).toInt, items(3), items(4), items(5).toInt)
    }).toDS()

    ds.show(100)

    ds.createOrReplaceTempView("studentScore")

    spark.sql("select age_20, count(distinct name) as cnt from (select name, case when age > 20 then 'G20' else (case when age < 20 then 'L20' else 'E20' end) end as age_20 from studentScore) group by age_20").show()

    spark.sql("select gender, count(distinct name) as cnt from studentScore group by gender").show()

    spark.sql("select classNo, count(distinct name) as cnt from studentScore group by classNo").show()

    spark.sql("select subject, avg(score) as avg_score from studentScore group by subject").show()

    spark.sql("select name, avg(score) as avg_score from studentScore group by name").show()

    spark.sql("select classNo, avg(score) as avg_score from studentScore group by classNo").show()

    spark.sql("select classNo, gender, avg(score) as avg_score from studentScore group by classNo, gender").show()

    spark.sql("select subject, max(score) as max_score from studentScore group by subject").show()

    spark.sql("select classNo, subject, max(score) as max_score, min(score) as min_score from studentScore group by classNo, subject").show()

    spark.stop()

  }
}
