import org.apache.hive.service.cli.HiveSQLException
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object OpenwidowFunctionDemo extends App {

  val sparkConf = new SparkConf().setMaster("local").setAppName("openwindowFunctionDemo")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new HiveContext(sc)

  sqlContext.sql("use default")
//  sqlContext.sql("drop table if exists user")
//  sqlContext.sql("create table if not exists user (id string,age int,sellerId string,dt TIMESTAMP) row format delimited fields terminated by ','")
//  sqlContext.sql("load data local inpath '/Users/chenshuai1/github/MySpark/src/main/resources/user.txt' into table user")

//  sqlContext.sql("select * from user").show

  val sql = "select id, age, sellerId, dt, next_time, datediff(dt, next_time) as dd from (" +
    "select * from (" +
    "   SELECT id, age, sellerId, dt, ROW_NUMBER() OVER (PARTITION BY sellerId ORDER BY dt DESC) rank, " +
    "   LEAD(dt,1) OVER(PARTITION BY sellerId ORDER BY dt desc) AS next_time FROM user" +
    ") where rank = 1)"

  //执行查询
  val df = sqlContext.sql(sql)

  df.show

  //将结果写入Hive表中
//  df.write.mode(SaveMode.Overwrite).saveAsTable("user_top3_res")

  sc.stop()
}
