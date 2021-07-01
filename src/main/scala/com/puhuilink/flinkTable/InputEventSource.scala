package com.puhuilink.flinkTable

import java.util

import org.apache.flink.api.common.typeinfo._
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.sources.{DefinedRowtimeAttributes, RowtimeAttributeDescriptor, StreamTableSource}
import org.apache.flink.types.Row

object InputEventSource extends StreamTableSource[Row] with DefinedRowtimeAttributes{
  //实现streamtablesource接口中的getDateStream（）方法，定义数入数据源
  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = ???
  //定义Table API中的时间属性信息
  override def getRowtimeAttributeDescriptors: util.List[RowtimeAttributeDescriptor] = ???
  //定义数据类型和数据集
  override def getReturnType: TypeInformation[Row] = {
    val names: Array[String] = Array[String]("name", "date1", "order1")
    val types: Array[TypeInformation[_]] = Array[TypeInformation[_]](Types.STRING,Types.SQL_TIMESTAMP,Types.INT)
    Types.ROW()
  }
  //阿哲是啥？
  override def getTableSchema: TableSchema = ???
}
