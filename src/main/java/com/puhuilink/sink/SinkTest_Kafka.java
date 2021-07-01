package com.puhuilink.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/1 15:10
 * @description：
 * @modified By：
 * @version: $
 */
public class SinkTest_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        DataStream<String> stringDataStream = executionEnvironment.readTextFile("C:\\Users\\无敌大大帅逼\\IdeaProjects\\flink-test\\flinkdemo\\src\\main\\resources\\Sensor.csv");

        stringDataStream.addSink(new FlinkKafkaProducer011<String>("cdh1:9092,cdh2:9092,cdh3:9092","test6",new SimpleStringSchema()));

        executionEnvironment.execute();

    }
}
