package com.puhuilink.window;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.puhuilink.pojo.Metric;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/2 9:25
 * @description：
 * @modified By：
 * @version: $
 */
public class Window_TimeWindow {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","cdh1:9092,cdh2:9092,cdh3:9092");
        properties.setProperty("zookeeper.connect","cdh1:2181,cdh2:2181,cdh3:2181");
        properties.setProperty("group.id","flink");

        DataStreamSource<String> test6 = env.addSource(new FlinkKafkaConsumer011<String>("test6", new SimpleStringSchema(), properties));

        SingleOutputStreamOperator<Metric> map = test6.map(new RichMapFunction<String, Metric>() {
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
        map.print();

        map.keyBy("host_id").timeWindow(Time.seconds(10)).aggregate(new AggregateFunction<Metric, Integer, Integer>() {
            @Override
            public Integer createAccumulator() {
                return 0;
            }

            @Override
            public Integer add(Metric metric, Integer integer) {
                System.out.println(integer);
                return integer+1;
            }

            @Override
            public Integer getResult(Integer integer) {
                return integer;
            }

            @Override
            public Integer merge(Integer integer, Integer acc1) {
                return integer+acc1;
            }
        }).print();

        //全窗口函数
        map.keyBy("host_id").timeWindow(Time.seconds(15)).apply(new WindowFunction<Metric, Tuple3<Integer,Long,Long>, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple tuple, TimeWindow window, Iterable<Metric> input, Collector<Tuple3<Integer,Long,Long>> out) throws Exception {
                Long start = window.getStart();
                Long field = tuple.getField(0);
                Integer Count = IteratorUtils.toList(input.iterator()).size();
                out.collect(new Tuple3<Integer,Long,Long>(Count,field,start));
            }
        }).print("win2");



        env.execute();


    }

}
