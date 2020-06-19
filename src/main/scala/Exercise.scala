import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

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

case class StudentScore(classNo: Int, name: String, age: Int,
                        gender: String, subject: String, score: Int)

object Exercise {

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

    val age20count = ds.select($"name", when($"age" > 20, "G20").when($"age" < 20, "L20").otherwise("E20").alias("age20"))
      .groupBy("name", "age20").count().groupBy("age20").count().as("age20_count")
    age20count.show()

    val genderCount = ds.groupBy("gender", "name").count().groupBy("gender").count().as("gender_count")
    genderCount.show()

    val classCount = ds.groupBy("classNo", "name").count().groupBy("classNo").count().as("class_count")
    classCount.show()

    val subjectAvgScore = ds.groupBy("subject").avg("score").as("subject_avg_score")
    subjectAvgScore.show()

    val avgScore = ds.groupBy("name").avg("score").as("avg_score")
    avgScore.show()

    val classGenderAvgScore = ds.groupBy("classNo", "gender").avg("score").as("class_gender_avg_score")
    classGenderAvgScore.show()

    // 19. 全校语文成绩最高分是多少？
    val subjectMaxScore = ds.groupBy("subject").max("score")
    subjectMaxScore.show()

    // 20. 12班语文成绩最低分是多少？
    val classSubjectMinScore = ds.groupBy("classNo", "subject").min("score")
    classSubjectMinScore.show()

    // 21. 13班数学最高成绩是多少？
    val classSubjectMaxScore = ds.groupBy("classNo", "subject").max("score")
    classSubjectMaxScore.show()

    // 22. 总成绩大于150分的12班的女生有几个？
    val totalScore = ds.filter("gender = '女' and classNo = 12").groupBy("name").sum("score").where("sum(score) > 150")
    totalScore.show()

    // 23. 总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少？
    val ds1 = ds.filter("age >= 19 and subject = 'math' and score >= 70").select("name")
    val ds2 = ds1.join(ds, Seq("name"), "left").groupBy("name").sum("score").where("sum(score) > 150").select("name")
    val ds3 = ds2.join(ds, Seq("name"), "left").groupBy("name").avg("score")
    ds3.show()

    spark.stop()

  }

}
