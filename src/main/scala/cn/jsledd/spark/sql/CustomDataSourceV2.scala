package cn.jsledd.spark.sql

import java.util
import java.util.Optional

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType

/**
  * @author ：jsledd
  * @date ：Created in 2019/4/5 0001 上午 9:19
  * @description：自定义DataSourceV2 的数据源
  * @modified By：
  * @version: $version$
  */
class CustomDataSourceV2 extends DataSourceV2 with ReadSupport with WriteSupport {
  /**
    * 创建Reader
    *
    * @param dataSourceOptions 用户自定义的options
    * @return 返回自定义的DataSourceReader
    */
  override def createReader(dataSourceOptions: DataSourceOptions): DataSourceReader = ???

  /**
    * 创建Writer
    *
    * @param jobId   jobId
    * @param schema  schema
    * @param mode    保存模式
    * @param options 用于定义的option
    * @return Optional[自定义的DataSourceWriter]
    */
  override def createWriter(jobId: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = ???

}

/**
  * @author ：jsledd
  * @date ：Created in 2019/4/5 0001 下午 14:12
  * @description：${description}
  * @modified By：
  * @version: $version$
  */
case class CustomDataSourceV2Reader(options: Map[String, String]) extends DataSourceReader {
  /**
    * 读取的列相关信息
    *
    * @return
    */
  override def readSchema(): StructType = ???

  /**
    * 每个分区拆分及读取逻辑
    *
    * @return
    */
  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = ???
}

class CustomDataSourceWriter(dataSourceOptions: DataSourceOptions) extends DataSourceWriter {
  /**
    * 创建RestDataWriter工厂类
    *
    * @return DataWriterFactory
    */
  override def createWriterFactory(): DataWriterFactory[InternalRow] = ???

  /**
    * commit
    *
    * @param writerCommitMessages 所有分区提交的commit信息
    *                             触发一次
    */
  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = ???

  /** *
    * abort
    *
    * @param writerCommitMessages 当write异常时调用,该方法用于事务回滚，当write方法发生异常之后触发该方法
    */
  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = ???
}





/**
  * @author ：jsledd
  * @date ：Created in 2019/11/1 0001 下午 14:21
  * @description：定义每个分区具体是如何读取的
  * @modified By：
  * @version: $version$
  */

case class CustomInputPartition(requiredSchema: StructType, pushed: Array[Filter], options: Map[String, String]) extends InputPartition[InternalRow] {

  override def createPartitionReader(): InputPartitionReader[InternalRow] = ???
}

case class CustomInputPartitionReader(requiredSchema: StructType, pushed: Array[Filter], options: Map[String, String]) extends InputPartitionReader[InternalRow] {

  override def next(): Boolean = ???

  override def get(): InternalRow = ???

  override def close(): Unit = ???
}

/**
  * DataWriterFactory工厂类
  */
class CustomDataWriterFactory extends DataWriterFactory[Row] {
  /**
    * 创建DataWriter
    *
    * @param partitionId 分区ID
    * @param taskId      task ID
    * @param epochId     一种单调递增的id，用于将查询分成离散的执行周期。对于非流式查询，此ID将始终为0。
    */
  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[Row] = ???
}

/**
  * RestDataWriter
  *
  * @param partitionId 分区ID
  */
class RestDataWriter(partitionId: Int) extends DataWriter[Row] {
  /**
    * write
    *
    * @param record 单条记录
    *               每条记录都会触发该方法
    */
  override def write(record: Row): Unit = ???

  /**
    * commit
    *
    * @return commit message
    *         每个分区触发一次
    */
  override def commit(): WriterCommitMessage = ???


  /**
    * 回滚：当write发生异常时触发该方法,该方法用于事务回滚，当write方法发生异常之后触发该方法
    */
  override def abort(): Unit = ???
}