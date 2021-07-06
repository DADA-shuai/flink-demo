package com.puhuilink.state;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.puhuilink.pojo.Metric;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/5 14:33
 * @description： 键控状态
 * @modified By：
 * @version: $
 */
public class State_KeyState {
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

        //定义一个有状态的map操作，统计sensor数据个数
        SingleOutputStreamOperator<Integer> map = dataStream.keyBy("host_id").map(new MyMapper());

        map.print();

        env.execute();
    }

    public static class MyMapper extends RichMapFunction<Metric,Integer>{
        private ValueState<Integer> myKeyCount = null;
        private ListState<Integer> myListState = null;
        private MapState<String,Integer> myMapState = null;
        private ReducingState<Integer> myReducingState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            myKeyCount=getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count",Integer.class));
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("list-count",Integer.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>("map-count",String.class,Integer.class));
            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>("reduce-count", new ReduceFunction<Integer>() {
                @Override
                public Integer reduce(Integer s, Integer t1) throws Exception {
                    return s+t1;
                }
            },Integer.class));
        }

        @Override
        public Integer map(Metric metric) throws Exception {
            Integer count = 0;
            if (myKeyCount.value() != null) {
                count = myKeyCount.value();
            }
            count++;
            myKeyCount.update(count);
            return count;
        }
    }
}
