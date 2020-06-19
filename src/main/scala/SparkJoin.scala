import org.apache.spark.sql.SparkSession

case class CodeCity(id: Int, city: String, province: String, event_time: String, is_deleted: Boolean)
case class CodeCityDelta(seq: Int, es: Long, ts: Long, optype: String, dt: String, id: Int, city: String, province: String, event_time: String)

object SparkJoin extends App {

  val spark = SparkSession.builder
    .appName("SparkJoin")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val codeCities = Seq(
     CodeCity(1, "南昌", "江西", "2020-05-13 14:55:36", false),
     CodeCity(2, "南京", "江苏", "2020-05-13 14:55:36", false)
  )

  val codeCityDeltas = Seq(
     CodeCityDelta(1, 1590545321000L, 1590545747450L, "INSERT", "20200527", 3, "上海", "上海", "2020-05-27 14:50:36"),
     CodeCityDelta(2, 1590545321000L, 1590545747450L, "UPDATE", "20200527", 2, "无锡", "江苏", "2020-05-27 14:51:36"),
     CodeCityDelta(3, 1590545322000L, 1590545747450L, "UPDATE", "20200527", 2, "苏州", "江苏", "2020-05-27 14:52:36"),
     CodeCityDelta(4, 1590545323000L, 1590545747450L, "DELETE", "20200527", 1, "南昌", "江西", "2020-05-27 14:53:36")
  )

  import spark.sqlContext.implicits._

  val codeCityDF = codeCities.toDF()
  val codeCityDeltaDF = codeCityDeltas.toDF()

  codeCityDF.show()
//  codeCityDF.printSchema()
//
  codeCityDeltaDF.show()
//  codeCityDeltaDF.printSchema()

  codeCityDF.createOrReplaceTempView("code_city")
  codeCityDeltaDF.createOrReplaceTempView("code_city_delta")

  val query =
    """
      |SELECT COALESCE(t2.id, t1.id) AS id,
      |       COALESCE (t2.city, t1.city) AS city,
      |       COALESCE (t2.province, t1.province) AS province,
      |       COALESCE (t2.event_time, t1.event_time) AS event_time,
      |       COALESCE (t2.optype == 'DELETE', t1.is_deleted) AS is_deleted
      |FROM code_city t1
      |FULL OUTER JOIN
      |  (
      |    SELECT id,
      |          city,
      |          province,
      |          event_time,
      |          optype
      |    FROM
      |     (
      |        SELECT id,
      |             city,
      |             province,
      |             event_time,
      |             optype,
      |             row_number () OVER (PARTITION BY id ORDER BY event_time DESC) AS rank
      |        FROM code_city_delta
      |        WHERE dt = '20200527'
      |     ) TEMP
      |     WHERE rank = 1
      |  ) t2 ON t1.id = t2.id
      |""".stripMargin

  val df = spark.sql(query)
  df.show()

}
