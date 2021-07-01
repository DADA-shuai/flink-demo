package com.puhuilink

import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object ValueStatus {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val value: DataStreamSource[String] = env.socketTextStream("cdh1", 10086, "\n")

    env.execute("sss")
  }

}
