package com.iteblog.mysql

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD}
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.{EqualTo, Filter, GreaterThan, IsNotNull}
import org.apache.spark.sql.types.StructType
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class MySQLSourceReader(options: Map[String, String]) extends DataSourceReader
  with SupportsPushDownRequiredColumns with SupportsPushDownFilters{

  val supportedFilters: ArrayBuffer[Filter] = ArrayBuffer[Filter]()

  val requiredSchema: StructType = {
    val jdbcOptions = new JDBCOptions(options)
    JDBCRDD.resolveTable(jdbcOptions)
  }

  override def readSchema(): StructType = {
    requiredSchema
  }

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    List[InputPartition[InternalRow]](MySQLInputPartition(requiredSchema, supportedFilters.toArray, options)).asJava
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    if (filters.isEmpty) {
      return filters
    }

    val unsupportedFilters = ArrayBuffer[Filter]()

    filters.foreach {
      case f: EqualTo => supportedFilters += f
      case f: GreaterThan => supportedFilters += f
      case f: IsNotNull => supportedFilters += f
      case f@_ => unsupportedFilters += f
    }

    unsupportedFilters.toArray
  }

  override def pushedFilters(): Array[Filter] = supportedFilters.toArray

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }
}
