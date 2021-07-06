package com.puhuilink.faultTolerant;

import com.puhuilink.pojo.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/6 11:42
 * @description：
 * @modified By：
 * @version: $
 */
public class Test_checkPoint {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.设置checkPoint
        executionEnvironment.enableCheckpointing(300);

        //2.高级选项
        executionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        executionEnvironment.getCheckpointConfig().setCheckpointTimeout(60000L);
        //前一个checkpoint还没处理完就开始下一个checkpoint
        executionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //在两个checkpoint之前最小间隔时间 300+100
        executionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(100);
        //false:选择checkpoint和savepoint两个中近的那个恢复， true： 只选择checkpoint
        executionEnvironment.getCheckpointConfig().setPreferCheckpointForRecovery(false);
        //容忍checkpoint保存失败的次数
        executionEnvironment.getCheckpointConfig().setTolerableCheckpointFailureNumber(1);


        //3.重启策略配置
        //固定延时重启
        executionEnvironment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10000L));
        //失败率重启
        executionEnvironment.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10),Time.minutes(1)));

        executionEnvironment.setParallelism(1);
        DataStream<String> stringDataStream = executionEnvironment.readTextFile("C:\\Users\\无敌大大帅逼\\IdeaProjects\\flink-test\\flinkdemo\\src\\main\\resources\\Sensor.csv");
        SingleOutputStreamOperator<SensorReading> dataStream = stringDataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
            }
        });

        executionEnvironment.execute();
    }
}
