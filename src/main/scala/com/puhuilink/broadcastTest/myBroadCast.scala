package com.puhuilink.broadcastTest

import org.apache.flink.api.scala._

object myBroadCast {

  def main(args: Array[String]): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    env.registerCachedFile("hdfs:///tmp/data","people.txt")

    val dataSet1: DataSet[Int] = env.fromElements(1,2,3)

    val dataSet2: DataSet[String] = env.fromElements("yjj", "dsb", "xtc")

//    dataSet2.map(new RichMapFunction[String,String] {
//      var broadcastSet = null
//
//      override def map(in: String): String = {
//        getRuntimeContext.getBroadcastVariable("broadcastSet-1").asScala
//        getRuntimeContext.getDistributedCache.getFile("people.txt")
//      }
//    })

  }

}
