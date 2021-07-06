package com.puhuilink.process;

import com.puhuilink.pojo.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/6 9:35
 * @description：
 * @modified By：
 * @version: $
 */
public class Test2_ProcessFunctionApp {
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

        dataStream.keyBy("id").process(new MyProcess(10));


        executionEnvironment.execute();
    }

    private static class MyProcess extends KeyedProcessFunction<Tuple,SensorReading,String> {
        private Integer interval;

        private ValueState<Double> lastTempState;

        private ValueState<Long> timerTsState;

        public MyProcess(Integer interval){
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp",Double.class,Double.MIN_VALUE));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-state",Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            Double lastTemp = lastTempState.value();
            Long timerTs = timerTsState.value();

            if (value.getTemperature()>lastTemp && timerTs == null) {
                Long ts = ctx.timerService().currentProcessingTime() + interval * 1000L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                timerTsState.update(ts);
            } else if (value.getTemperature()<lastTemp && timerTs != null) {
                ctx.timerService().deleteEventTimeTimer(timerTs);
            }

            //更新温度状态
            lastTempState.update(value.getTemperature());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("传感器"+ctx.getCurrentKey().getField(0)+"温度值连续"+interval+"s上升");
            timerTsState.clear();
        }


    }
}
