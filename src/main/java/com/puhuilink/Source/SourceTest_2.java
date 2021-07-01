package com.puhuilink.Source;

import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;

/**
 * @author ：yjj
 * @date ：Created in 2021/6/30 17:53
 * @description：file
 * @modified By：
 * @version: $
 */
public class SourceTest_2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        DataStream<String> stringDataStream = executionEnvironment.readTextFile("C:\\Users\\无敌大大帅逼\\IdeaProjects\\flink-test\\flinkdemo\\src\\main\\resources\\Sensor.csv");
        stringDataStream.print();
        executionEnvironment.execute();
    }
}
