package com.puhuilink.state;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.puhuilink.pojo.Metric;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/5 15:35
 * @description：
 * @modified By：
 * @version: $
 */
public class State_KeyStateApplication {
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
        SingleOutputStreamOperator<Tuple3<String,Double,Double>> map = dataStream.keyBy("host_id").flatMap(new TempChangeWarning(10.0));

        map.print();

        env.execute();
    }

    private static class TempChangeWarning extends RichFlatMapFunction<Metric, Tuple3<String,Double,Double>> {
        private Double warning ;
        private ValueState<Double> lastTempState= null;

        public TempChangeWarning(double v) {
            this.warning = v;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp",Double.class));
        }

        @Override
        public void flatMap(Metric metric, Collector<Tuple3<String, Double, Double>> collector) throws Exception {
            Double lastTemp = lastTempState.value();
            if (lastTemp !=null) {
                double diff = Math.abs(metric.getMetric_value() - lastTemp);
                if (diff>=warning) {
                    collector.collect(new Tuple3<>(metric.getHost_id().toString(),lastTemp,metric.getMetric_value()));
                }
            }
            //更新状态的值
            lastTempState.update(metric.getMetric_value());
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
            super.close();
        }
    }
}
