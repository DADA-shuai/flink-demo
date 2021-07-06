package com.puhuilink.process;

import com.puhuilink.pojo.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/6 10:03
 * @description：
 * @modified By：
 * @version: $
 */
public class Test3_SideOutPut {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        DataStream<String> stringDataStream = executionEnvironment.readTextFile("C:\\Users\\无敌大大帅逼\\IdeaProjects\\flink-test\\flinkdemo\\src\\main\\resources\\Sensor.csv");
        SingleOutputStreamOperator<SensorReading> dataStream = stringDataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
            }
        });

        OutputTag<SensorReading> sensorReadingOutputTag = new OutputTag<SensorReading>("low-temp") {};

        SingleOutputStreamOperator<SensorReading> high = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                if (value.getTemperature() < 30) {
                    ctx.output(sensorReadingOutputTag, value);
                }else {
                    out.collect(value);
                }
            }
        });

        high.getSideOutput(sensorReadingOutputTag).print("low");
        high.print("high");

        executionEnvironment.execute();
    }
}
