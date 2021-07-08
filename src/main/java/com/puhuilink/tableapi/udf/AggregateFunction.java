package com.puhuilink.tableapi.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/7 16:27
 * @description：
 * @modified By：
 * @version: $
 */
public class AggregateFunction {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env,blinkStreamSettings);


        blinkStreamTableEnv.connect(new FileSystem().path("C:\\Users\\无敌大大帅逼\\IdeaProjects\\flink-test\\flinkdemo\\src\\main\\resources\\Sensor.csv"))
            .withFormat(new Csv().fieldDelimiter(','))
            .withSchema(new Schema().field("id", DataTypes.STRING())
                .field("ts",DataTypes.BIGINT())
                .field("temp",DataTypes.DOUBLE()))
            .createTemporaryTable("sensorTable");

        Table sensorTable = blinkStreamTableEnv.from("sensorTable");

        //注册
        blinkStreamTableEnv.registerFunction("avgTemp",new AvgTemp());

        //自定义标量函数，实现求id的hash值
        //table api
        Table result = sensorTable.groupBy("id").aggregate("avgTemp(temp) as avgtemp").select("id,avgtemp");

        //sql
        Table sql = blinkStreamTableEnv.sqlQuery("select id,avgTemp(temp) from sensorTable group by id");

        blinkStreamTableEnv.toRetractStream(result, Row.class).print("table");
        blinkStreamTableEnv.toRetractStream(sql, Row.class).print("sql");

        env.execute();

    }

    public static class AvgTemp extends org.apache.flink.table.functions.AggregateFunction<Double, Tuple2<Double,Integer>> {


        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0/accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0,0);
        }

        /**
         * 方法名和参数的顺序、类型都不能变，且必须有这个方法
         * @param accumulator
         * @param temp
         */
        public void accumulate(Tuple2<Double, Integer> accumulator,Double temp){
            accumulator.f0 += temp;
            accumulator.f1 += 1;
        }
    }

    public static class TableAvgTemp extends TableAggregateFunction<Double, List<Double>> {

        @Override
        public List<Double> createAccumulator() {
            return new ArrayList<Double>();
        }

        public void emitValue() {

        }

        public void accumulate(List<Double> alist){

        }

    }
}
