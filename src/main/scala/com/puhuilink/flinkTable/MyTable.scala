package com.puhuilink.flinkTable

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row

object MyTable {

  def main(args: Array[String]): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val bte: BatchTableEnvironment = null

//    bte.connect(new FileSystem().path("C:\\Users\\无敌大大帅逼\\OneDrive\\文档\\Tencent Files\\order.csv"))
//      .withFormat(new Csv()
//        .field("name",Types.STRING)
//        .field("date1",Types.SQL_TIMESTAMP)
//        .field("order1",Types.INT))
//      .withSchema(new Schema()
//          .field("name",Types.STRING)
//          .field("date1",Types.SQL_TIMESTAMP)
//          .field("order1",Types.INT))
//      .registerTableSource("CsvTable")

    val table: Table = bte.sqlQuery(
      """
        |select name,date1,order1 from CsvTable
        |""".stripMargin)

    val value: DataSet[Row] = bte.toDataSet[Row](table)

    value.map(m=>(m.getField(0),m.getField(1),m.getField(2))).print()

//    val source = new CsvTableSource("C:\\Users\\无敌大大帅逼\\OneDrive\\文档\\Tencent Files\\order.csv", ("name", "date", "order"), (Types.STRING, Types.STRING, Types.INT))
  }

}
