//package com.iteblog.mysql
//
//import org.apache.spark.sql.sources.v2.reader.DataSourceReader
//import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
//
//import scala.collection.JavaConverters._
//
//class DefaultSource extends DataSourceV2 with ReadSupport {
//  override def createReader(dataSourceOptions: DataSourceOptions): DataSourceReader = {
//    MySQLSourceReader(dataSourceOptions.asMap().asScala.toMap)
//  }
//}
