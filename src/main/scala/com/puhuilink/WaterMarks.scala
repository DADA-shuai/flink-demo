package com.puhuilink

import java.util.Properties

import com.google.gson.GsonBuilder
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object WaterMarks {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)


    val properties = new Properties()
    properties.setProperty("bootstrap.servers","cdh1:9092,cdh2:9092,cdh3:9092")
    properties.setProperty("zookeeper.connect","cdh1:2181,cdh2:2181,cdh3:2181")
    properties.setProperty("group.id","flink")

    val value: SingleOutputStreamOperator[Metric] = env.addSource(new FlinkKafkaConsumer010[String]("test6", new SimpleStringSchema(), properties))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(10)) {
        override def extractTimestamp(element: String): Long = {
          val gson = new GsonBuilder().serializeNulls().create()
          gson.fromJson(element, classOf[Metric]).upload_time
        }
      }).map(new MapFunction[String, Metric] {
        override def map(t: String): Metric = {
          val gson = new GsonBuilder().serializeNulls().create()
          gson.fromJson(t, classOf[Metric])
        }
      })

    value.timeWindowAll(Time.seconds(1))

    value.timeWindowAll(Time.seconds(10),Time.seconds(1))

    value.windowAll(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(10)))

    value.windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))

    value.print()


//    val value1: DataStreamSource[(String, Long, Int)] = env.fromCollection(util.Arrays.asList(("a", 1L, 1), ("b", 1L, 1), ("b", 3L, 1)))

//    value1.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long, Int)] {
//      val macOutOfOrderness = 1000L
//
//      var currentMaxTimestamp:Long  = _
//
//      override def getCurrentWatermark: Watermark = {
//        new Watermark(currentMaxTimestamp - macOutOfOrderness)
//      }
//
//      override def extractTimestamp(t: (String, Long, Int), l: Long): Long = {
//        val currentTimestamp = t._2
//        Math.max(currentMaxTimestamp,currentTimestamp)
//      }
//    })

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
