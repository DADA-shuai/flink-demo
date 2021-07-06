package com.puhuilink.window;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.puhuilink.pojo.Metric;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.OutputTag;
import org.postgresql.core.Tuple;
import scala.Tuple2;

import java.util.Properties;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/2 13:55
 * @description：
 * @modified By：
 * @version: $
 */
public class Window_TimeCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.setParallelism(1);
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

        SingleOutputStreamOperator<Double> host_id = map.keyBy("host_id").countWindow(20, 3)
            .aggregate(new AggregateFunction<Metric, Tuple2<Integer, Double>, Double>() {
                @Override
                public Tuple2<Integer, Double> createAccumulator() {
                    return new Tuple2<Integer, Double>(0, 0.0);
                }

                @Override
                public Tuple2<Integer, Double> add(Metric metric, Tuple2<Integer, Double> integerDoubleTuple2) {
                    return new Tuple2<>(integerDoubleTuple2._1 + 1, integerDoubleTuple2._2 + metric.getMetric_value());
                }

                @Override
                public Double getResult(Tuple2<Integer, Double> integerDoubleTuple2) {
                    return integerDoubleTuple2._2 / integerDoubleTuple2._1;
                }

                @Override
                public Tuple2<Integer, Double> merge(Tuple2<Integer, Double> integerDoubleTuple2, Tuple2<Integer, Double> acc1) {
                    return null;
                }
            });

        host_id.print();

        SingleOutputStreamOperator<Metric> sum = map.keyBy("host_id").timeWindow(Time.seconds(15))
//            .trigger(new Trigger<Metric, TimeWindow>() {})  定义window 什么时候关闭，触发计算并输出结果，一般用于强行关闭窗口
//            .evictor(new Evictor<Metric, TimeWindow>() {})  过滤某些数据
//            .allowedLateness(Time.minutes(1))  让窗口保留一分钟 结束延迟数据，一分钟后触发输出结果，关闭窗口
//            .sideOutputLateData(new OutputTag<Metric>("late"))  实在迟到的数据可以放到侧输出流中，通过其他方式获取再计算
            .sum("metric_value");

        DataStream<Metric> late = sum.getSideOutput(new OutputTag<Metric>("late"));

        env.execute();
    }
}
