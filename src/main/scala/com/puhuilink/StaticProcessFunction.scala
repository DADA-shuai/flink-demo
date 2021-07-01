package com.puhuilink

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class StaticProcessFunction extends ProcessWindowFunction[(String,Long,Int),(String,Long,Long,Long,Long,Long),String,TimeWindow]{
  override def process(key: String, context: Context, elements: Iterable[(String, Long, Int)], out: Collector[(String, Long, Long, Long, Long, Long)]): Unit = {
    val sum: Long = elements.map(_._2).sum
    val min: Long = elements.map(_._2).min
    val max: Long = elements.map(_._2).max
    val avg: Long = sum / elements.size
    val end: Long = context.window.getEnd
    out.collect((key,sum,min,max,avg,end))
  }
}
