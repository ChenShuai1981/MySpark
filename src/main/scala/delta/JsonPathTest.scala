package delta

import java.util

import com.jayway.jsonpath.{DocumentContext, JsonPath}
import net.minidev.json.JSONArray
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

object JsonPathTest {

  def parseSchema(json: String): StructType = {
    val dc = JsonPath.parse(json)
    val op: String = dc.read("$.payload.op")

    val records: JSONArray = dc.read("$.schema.fields[?(@.field == 'after')]")
    val record: java.util.LinkedHashMap[String, Any] = records.get(0).asInstanceOf[java.util.LinkedHashMap[String, Any]]
    val name: String = record.get("name").toString
    val fields: JSONArray = record.get("fields").asInstanceOf[JSONArray]
    val structFields = fields.map { f => {
      val fs = f.asInstanceOf[util.LinkedHashMap[String, Any]]
      val dataType = fs.get("type") match {
        case "string" => StringType
        case "int32" => LongType
        case t @ _ => throw new IllegalArgumentException("Not supported AttributeType: " + t.getClass.getName)
      }
      StructField(fs.get("field").asInstanceOf[String], dataType, fs.get("optional").asInstanceOf[Boolean])
    }}

    StructType(structFields)
  }

  def getData(json: String): List[String] = {
    val data: util.LinkedHashMap[String, String] = JsonPath.parse(json).read("$.payload.after")
    data.values().toList
  }

}
