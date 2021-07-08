package com.puhuilink.tableapi.udf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/7 16:27
 * @description：
 * @modified By：
 * @version: $
 */
public class ScalarFunction {
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
        blinkStreamTableEnv.registerFunction("hashCode",new MyUdfFunction());
        //自定义标量函数，实现求id的hash值
        //table api
        Table result = sensorTable.select("id,ts,hashCode(id)");

        //sql
        Table sql = blinkStreamTableEnv.sqlQuery("select id,ts,hashCode(id) from sensorTable");

        blinkStreamTableEnv.toAppendStream(result, Row.class).print("table");
        blinkStreamTableEnv.toAppendStream(sql, Row.class).print("sql");

        env.execute();

    }

    public static class MyUdfFunction extends org.apache.flink.table.functions.ScalarFunction {

        private int factor = 13;

        public MyUdfFunction(){}

        public MyUdfFunction(int factor) {
            this.factor = factor;
        }

        public int eval(String str) {
           return str.hashCode() * factor;
        }
    }
}
