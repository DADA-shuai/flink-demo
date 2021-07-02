package com.puhuilink.sink;

import com.puhuilink.pojo.SensorReading;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;


import java.util.ArrayList;
import java.util.HashMap;


/**
 * @author ：yjj
 * @date ：Created in 2021/7/1 15:10
 * @description：
 * @modified By：
 * @version: $
 */
public class SinkTest_Es {
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

        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("cdh3",9200));


        map.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts,new MyEsSinkMapper()).build());

        executionEnvironment.execute();

    }

    private static class MyEsSinkMapper implements org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction<SensorReading> {
        @Override
        public void process(SensorReading sensorReading, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            HashMap<String, String> stringSensorReadingHashMap = new HashMap<>();
            stringSensorReadingHashMap.put("id",sensorReading.getId());
            stringSensorReadingHashMap.put("temp",sensorReading.getTemperature().toString());
            stringSensorReadingHashMap.put("ts",sensorReading.getTimestamp().toString());

            //创建es请求
            IndexRequest source = Requests.indexRequest()
                .index("sensor")
                .type("readingdata")
                .source(stringSensorReadingHashMap);

            requestIndexer.add(source);
        }
    }
}
