package com.iteblog.mysql

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.delta.util.DateTimeUtils
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.sources.{EqualTo, Filter, GreaterThan, IsNotNull}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class MySQLInputPartition(requiredSchema: StructType, pushed: Array[Filter], options: Map[String, String]) extends InputPartition[InternalRow] {
  override def createPartitionReader(): InputPartitionReader[InternalRow] = MySQLInputPartitionReader(requiredSchema, pushed, options)
}

case class MySQLInputPartitionReader(requiredSchema: StructType, pushed: Array[Filter], options: Map[String, String])
  extends InputPartitionReader[InternalRow] {

  val tableName: String = options("dbtable")
  val driver: String = options("driver")
  val url: String = options("url")
  val user: String = options("user")
  val password: String = options("password")

  def initSQL: String = {
    val selected = if (requiredSchema.isEmpty) "1" else requiredSchema.fieldNames.mkString(",")

    if (pushed.nonEmpty) {
      val dialect = JdbcDialects.get(url)
      val filter = pushed.map {
        case EqualTo(attribute, value) => s"${dialect.quoteIdentifier(attribute)} = ${dialect.compileValue(value)}"
        case GreaterThan(attribute, value) => s"${dialect.quoteIdentifier(attribute)} > ${dialect.compileValue(value)}"
        case IsNotNull(attribute) => s"${dialect.quoteIdentifier(attribute)} IS NOT NULL"
      }.mkString(" AND ")

      s"SELECT $selected FROM $tableName WHERE $filter"
    } else {
      s"SELECT $selected FROM $tableName"
    }
  }

  val rs: ResultSet = {
    Class.forName(driver)
    val conn = DriverManager.getConnection(url, user, password)
    println(initSQL)
    val stmt = conn.prepareStatement(initSQL, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    stmt.setFetchSize(1000)
    stmt.executeQuery()
  }

  override def next(): Boolean = rs.next()

  override def get(): InternalRow = {
    InternalRow(requiredSchema.fields.zipWithIndex.map { element =>
      element._1.dataType match {
        case IntegerType => rs.getInt(element._2 + 1)
        case DoubleType => rs.getDouble(element._2 + 1)
        case LongType => rs.getLong(element._2 + 1)
        case StringType => UTF8String.fromString(rs.getString(element._2 + 1))
        case e: DecimalType =>
          val d = rs.getBigDecimal(element._2 + 1)
          Decimal(d, d.precision(), d.scale())
        case TimestampType =>
          val t = rs.getTimestamp(element._2 + 1)
          DateTimeUtils.fromJavaTimestamp(t)
      }
    }: _*)
  }

  override def close(): Unit = rs.close()
}
