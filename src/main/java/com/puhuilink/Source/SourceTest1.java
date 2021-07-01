package com.puhuilink.Source;

import com.puhuilink.Source.pojo.SensorReading;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author ：yjj
 * @date ：Created in 2021/6/30 17:37
 * @description：Collection
 * @modified By：
 * @version: $
 */
public class SourceTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        DataStreamSource<SensorReading> sensorReadingDataStreamSource = executionEnvironment.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.7),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        ));
        sensorReadingDataStreamSource.print();
        JobExecutionResult test = executionEnvironment.execute("test");
    }

}
