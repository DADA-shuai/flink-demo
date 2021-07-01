package com.puhuilink.iterate

import org.apache.flink.api.scala._

object quanliangdirtdai {

  def main(args: Array[String]): Unit = {


    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val value: DataSet[Int] = environment.fromElements(0)

    val count : DataSet[Int] = value.iterate(10000) {
      iterationInput =>
        val result: DataSet[Int] = iterationInput.map(i => {
          val x: Double = Math.random()
          val y: Double = Math.random()
          i + (if (x * x + y * y < 1) 1 else 0)
        })
        result
    }

    val result = count map { c => c / 10000.0 * 4 }
    result.print()
  }

}
