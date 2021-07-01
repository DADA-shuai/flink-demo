package com.puhuilink

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import scala.util.Random

object customPartitioner extends Partitioner[String] {
  private val random: Random.type = scala.util.Random
  override def partition(k: String, i: Int): Int = {
    if (k.contains("flink"))
      0
    else
      random.nextInt(i)
  }
}

object WordCountStream {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment



    //自定义分区策略
//    val value = env.fromElements(3, 1, 2, 1, 5)
//
//    value.partitionCustom(customPartitioner,0)



    //不同数据集连接
//    val value1: DataStream[(String, Int)] = env.fromElements(("a", 3), ("d", 4), ("c", 2), ("c", 5), ("a", 5), ("d", 6))
//
//    val value2: DataStream[Int] = env.fromElements(1, 2, 3, 4, 6, 7)
//
//    val value: ConnectedStreams[(String, Int), Int] = value1.connect(value2)
//
//    value.map(new CoMapFunction[(String, Int), Int, (Int, String)] {
//      override def map1(in1: (String, Int)): (Int, String) = {
//        (in1._2, in1._1)
//      }
//
//      override def map2(in2: Int): (Int, String) = {
//        (in2, "default")
//      }
//    }).returns(TypeInformation.of(new TypeHint[Tuple2[Int ,String]] {})).print

//    val value: DataStreamSource[String] = env.socketTextStream("cdh1", 10086)
//
//    value.print()
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
