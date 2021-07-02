package com.puhuilink.Source;

import com.puhuilink.pojo.SensorReading;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Random;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/1 9:36
 * @description： UDF
 * @modified By：
 * @version: $
 */
public class SourceTest_UDF {
    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        DataStream<SensorReading> stringDataStream = executionEnvironment.addSource(new MySource(),null);
        stringDataStream.print();
        executionEnvironment.execute();
    }

    public static class MySource extends RichSourceFunction<SensorReading> {
        //定义标志位
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            Random random = new Random();

            HashMap<String, Double> stringDoubleHashMap = new HashMap<>();

            for (int i = 0;i<10;i++ ){
                stringDoubleHashMap.put("sensor_"+(i+1),60 + random.nextGaussian()*20 );
            }

            while(running) {

                for (String s : stringDoubleHashMap.keySet()) {
                    double v = stringDoubleHashMap.get(s) + random.nextGaussian();
                    stringDoubleHashMap.put(s,v);
                    ctx.collect(new SensorReading(s,System.currentTimeMillis(),v));
                }

                Thread.sleep(1000);

            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
