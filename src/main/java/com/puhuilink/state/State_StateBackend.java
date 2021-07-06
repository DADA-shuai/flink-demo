package com.puhuilink.state;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.puhuilink.pojo.Metric;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/5 16:31
 * @description： 状态后端 ，memory filesystem rockdb
 * @modified By：
 * @version: $
 */
public class State_StateBackend {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1. 状态后端配置
        //内存
        env.setStateBackend(new MemoryStateBackend());
        //文件系统 in : uri
        env.setStateBackend(new FsStateBackend(""));
        //rocksDb in : uri
        env.setStateBackend(new RocksDBStateBackend(""));

//        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","cdh1:9092,cdh2:9092,cdh3:9092");
        properties.setProperty("zookeeper.connect","cdh1:2181,cdh2:2181,cdh3:2181");
        properties.setProperty("group.id","flink");

        DataStreamSource<String> test6 = env.addSource(new FlinkKafkaConsumer011<String>("test6", new SimpleStringSchema(), properties));

        SingleOutputStreamOperator<Metric> dataStream = test6.map(new RichMapFunction<String, Metric>() {
            private Gson gson = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                gson = new GsonBuilder().create();
            }

            @Override
            public Metric map(String s) throws Exception {
                return gson.fromJson(s, Metric.class);
            }
        });

        env.execute();

    }
}
