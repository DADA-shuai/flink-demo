package com.puhuilink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream._
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.table.api.java.StreamTableEnvironment

object KafkaStream {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    val tableEnv: StreamTableEnvironment = null

    val properties = new Properties()
    properties.setProperty("bootstrap.servers","cdh1:9092,cdh2:9092,cdh3:9092")
    properties.setProperty("zookeeper.connect","cdh1:2181,cdh2:2181,cdh3:2181")
    properties.setProperty("group.id","flink")

    val value: DataStreamSource[String] = env.addSource(new FlinkKafkaConsumer011[String]("test6", new SimpleStringSchema(), properties))

    tableEnv.registerDataStream("kafkaTest",value)

    env.execute("test")
//    val value: DataSet[String] = env.readFile(new CsvInputFormat[String](new Path("C:\\Users\\无敌大大帅逼\\OneDrive\\文档\\Tencent Files\\order.csv")) {
//      override def fillRecord(out: String, objects: Array[AnyRef]): String = {
//        return null
//      }
//    }, "C:\\Users\\无敌大大帅逼\\OneDrive\\文档\\Tencent Files\\order.csv")

//    value.print()
//    val dataset: DataSet[(String, Int)] = env.fromElements(("hello", 1), ("flink", 3), ("hello",3))
//
//    val value: AggregateDataSet[(String, Int)] = dataset.groupBy(0).max(1)
//
//    value.print()

//    val value: DataSet[Person] = env.fromElements(new Person("alex", 18), new Person("peter", 43), new Person("alex", 23))
//
//    value.map(_.name).print()
  }

}
