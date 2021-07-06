package com.puhuilink.process;

import com.puhuilink.pojo.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/5 17:55
 * @description：
 * @modified By：
 * @version: $
 */
public class Test1_KeydProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        DataStream<String> stringDataStream = executionEnvironment.readTextFile("C:\\Users\\无敌大大帅逼\\IdeaProjects\\flink-test\\flinkdemo\\src\\main\\resources\\Sensor.csv");
        SingleOutputStreamOperator<SensorReading> dataStream = stringDataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
            }
        });

        dataStream.keyBy("id").process(new MyProcess());


        executionEnvironment.execute();
    }

    private static class MyProcess extends KeyedProcessFunction<Tuple,SensorReading,Integer> {
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            getRuntimeContext().getState(new ValueStateDescriptor<Long>("key-state",Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<Integer> out) throws Exception {
            out.collect(value.getId().length());

            //侧输出流
            ctx.output(new OutputTag<String>(value.getId()),"value-id");

            //context
            ctx.timestamp();
            ctx.timerService().registerEventTimeTimer(value.getTimestamp()+1000L);//从1970年开始的毫秒值
            ctx.timerService().registerProcessingTimeTimer(value.getTimestamp()+1000L);
            ctx.timerService().deleteEventTimeTimer(value.getTimestamp()+1000L);//删除定时器
        }

        /***
         * 注册定时后，执行这个任务
         * @param timestamp
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            super.onTimer(timestamp, ctx, out);

        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }
}
