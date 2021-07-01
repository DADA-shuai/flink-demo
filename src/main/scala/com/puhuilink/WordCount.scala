package com.puhuilink


import org.apache.flink.api.scala._
import org.apache.flink.api.scala.hadoop.mapred.HadoopOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf, TextOutputFormat}


//case class Order(name:String,date:String,value:Int)

object WordCount {
  def main(args: Array[String]): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val value: DataSet[(Int, String, Int)] = env.fromElements((13, "Alice", 34), (11, "Boob", 36), (14, "Reboot", 37))

    val value1: DataSet[(String, Int)] = env.fromElements(("Alice", 420), ("Boob", 430), ("Reboot", 404))

    val word: DataSet[(Text, IntWritable)] = value1.map(t => (new Text(t._1), new IntWritable(t._2)))

    val hadoopOutputFormat= new HadoopOutputFormat[Text, IntWritable](
      new TextOutputFormat[Text, IntWritable],
      new JobConf()
    )

    FileOutputFormat.setOutputPath(hadoopOutputFormat.getJobConf, new Path("hdfs://cdh1:9000/tmp/examples/people.txt"))

    word.output(hadoopOutputFormat)

//    value.joinWithTiny(value1).where(1).equalTo(0){
//      (left,right) => (left._1,left._2,left._3,right._2+200)
//    }.writeAsText("C:\\Users\\无敌大大帅逼\\IdeaProjects\\flink-test\\flinkdemo\\person.txt")

//    val value: DataSet[Long] = env.fromElements(222,142,12341,14143)
//
//    value.reduceGroup(x => x.min).print()
//    env.fromElements("flink,spark,hadoop").flatMap(_.split(",")).print()

//    val value: DataSet[String] = env.fromElements("flink", "spark", "hadoop")
//
//    val value1: DataSet[String] = value.map(x => x.toUpperCase)
//
//    value1.print()
//    val value: DataSet[String] = env.fromElements("Who`is there?", "Hello World")

//    val value: DataSet[String] = env.readTextFile("hdfs://cdh1:9000/tmp/examples/people.txt")

//    val textPath = "hdfs://cdh1:9000/tmp/examples/people.txt"
//
//    val value: DataSet[Nothing] = env.createInput(HadoopInputs.readHadoopFile(
//      new TextInputFormat, classOf[Person], classOf[Text], textPath))

//    value.print()

//    val value1: AggregateDataSet[(String, Int)] = value.flatMap(_.toLowerCase.split("\\W+") filter (_.nonEmpty))
//      .map((_, 1))
//      .groupBy(0)
//      .sum(1)
//
//    value1.print()
//    val value1: DataSet[Order] = env.readCsvFile[Order]("C:\\Users\\无敌大大帅逼\\OneDrive\\文档\\Tencent Files\\order.csv")
//
//    val value: GroupedDataSet[Order] = value1.groupBy("name")
//
//    val value2: DataSet[Order] = value.reduce((t1, t2) => {
//      Order(t1.name, t1.date, t1.value + t2.value)
//    })
//
//    value2.setParallelism(1).writeAsText("order.txt")

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
