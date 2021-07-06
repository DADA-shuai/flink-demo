package com.puhuilink.state;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.puhuilink.pojo.Metric;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.expressions.In;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/2 17:48
 * @description：
 * @modified By：
 * @version: $
 */
public class State_Operator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


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

        //定义一个有状态的map操作，统计当前分区数据个数
        SingleOutputStreamOperator<Integer> map = dataStream.map(new MyMapper());

        map.print();

        env.execute();
    }

    private static class MyMapper implements MapFunction<Metric,Integer>, ListCheckpointed<Integer> {
        private Integer count = 0;
        @Override
        public Integer map(Metric metric) throws Exception {
            return ++count;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer integer : state) {
                count += integer;
            }
        }
    }
}
