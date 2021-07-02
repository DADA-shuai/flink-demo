package com.puhuilink.transform;

import com.puhuilink.pojo.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/1 10:41
 * @description：
 * @modified By：
 * @version: $
 */
public class Transform_key {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        DataStream<String> stringDataStream = executionEnvironment.readTextFile("C:\\Users\\无敌大大帅逼\\IdeaProjects\\flink-test\\flinkdemo\\src\\main\\resources\\Sensor.csv");

        SingleOutputStreamOperator<SensorReading> map = stringDataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
            }
        });

        KeyedStream<SensorReading, Tuple> id = map.keyBy("id");
//        map.keyBy(SensorReading::getId).max("temperature").print();
        SingleOutputStreamOperator<SensorReading> reduce = id.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading sensorReading, SensorReading t1) throws Exception {
                SensorReading sensorReading1 = new SensorReading();
                sensorReading1.setId(sensorReading.getId());
                sensorReading1.setTimestamp(Math.max(sensorReading.getTimestamp(), t1.getTimestamp()));
                sensorReading1.setTemperature(Math.max(sensorReading.getTemperature(), t1.getTemperature()));
                return sensorReading1;
            }
        });

        SingleOutputStreamOperator<SensorReading> reduce1 = id.reduce((oldData, newData) -> {
            return new SensorReading(oldData.getId(), Math.max(oldData.getTimestamp(), newData.getTimestamp()), Math.max(oldData.getTemperature(), newData.getTemperature()));
        });

        reduce.print("reduce");

        reduce1.print("reduce1");

        executionEnvironment.execute();

    }
}
