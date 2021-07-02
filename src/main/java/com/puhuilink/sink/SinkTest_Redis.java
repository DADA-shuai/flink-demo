package com.puhuilink.sink;

import com.puhuilink.pojo.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;


/**
 * @author ：yjj
 * @date ：Created in 2021/7/1 15:10
 * @description：
 * @modified By：
 * @version: $
 */
public class SinkTest_Redis {
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

        FlinkJedisPoolConfig build = new FlinkJedisPoolConfig.Builder().setHost("cdh1").setPort(7000).setPassword("redis123QAZ").build();

        map.addSink(new RedisSink<>(build,new MyRedisMapper()));

        executionEnvironment.execute();

    }

    private static class MyRedisMapper implements RedisMapper<SensorReading> {
        // 定义保存数据到redis的命令，存成hash表 ，hset sensor_temp
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"sensor_temp");
        }

        @Override
        public String getKeyFromData(SensorReading sensorReading) {
            return sensorReading.getId();
        }

        @Override
        public String getValueFromData(SensorReading sensorReading) {
            return sensorReading.toString();
        }
    }
}
