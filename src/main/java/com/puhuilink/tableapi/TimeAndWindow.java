package com.puhuilink.tableapi;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.puhuilink.pojo.Metric;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/7 14:01
 * @description：
 * @modified By：
 * @version: $
 */
public class TimeAndWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","cdh1:9092,cdh2:9092,cdh3:9092");
        properties.setProperty("zookeeper.connect","cdh1:2181,cdh2:2181,cdh3:2181");
        properties.setProperty("group.id","flink");

        DataStreamSource<String> kafkaData = env.addSource(new FlinkKafkaConsumer011<String>("test6", new SimpleStringSchema(), properties));

        SingleOutputStreamOperator<Metric> dataStream = kafkaData.map(new RichMapFunction<String, Metric>() {
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
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Metric>(Time.milliseconds(300)) {
            @Override
            public long extractTimestamp(Metric metric) {
                return metric.getUpload_time();
            }
        });

        //processTime
//        Table table = tableEnv.fromDataStream(dataStream, "agent, aggregate, collect_time, endpoint_id, host_id, metric_id, metric_value, metric_value_str, upload_time,pt.proctime");
        //eventime
        Table table = tableEnv.fromDataStream(dataStream, "agent, aggregate, collect_time, endpoint_id, host_id, metric_id, metric_value, metric_value_str, upload_time.rowtime");

        tableEnv.createTemporaryView("metric",table);

        //窗口操作
        Table result = table.window(Tumble.over("1.seconds").on("upload_time").as("tw"))
            .groupBy("host_id,tw")
            .select("host_id,host_id.count,metric_value.sum,tw.end");

        //SQL
        Table result1 = tableEnv.sqlQuery("select host_id, count(host_id) as cnt , sum(metric_value) as sumValue ,tumble_end(upload_time, interval '1' second) " +
            "from metric group by host_id,tumble(upload_time, interval '1' second)");

        //over操作
        //5.1 table api
        Table overTable = table.window(Over.partitionBy("host_id").orderBy("upload_time").preceding("2.rows").as("ow"))
            .select("host_id,upload_time,host_id.count over ow , metric_value.sum over ow");

        //5.2 sql
        Table overSqlTable = tableEnv.sqlQuery("select host_id,upload_time,count(host_id) over ow ,sum(metric_value) over ow " +
            "from metric " +
            "window ow as (partition by host_id order by upload_time rows between 2 preceding and current row)");



        tableEnv.toAppendStream(overTable, Row.class).print();
//        tableEnv.toAppendStream(overSqlTable, Row.class).print();



        env.execute();
    }
}
