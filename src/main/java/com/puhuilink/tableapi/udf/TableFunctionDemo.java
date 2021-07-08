package com.puhuilink.tableapi.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.postgresql.core.Tuple;

import java.security.ProtectionDomain;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/7 17:04
 * @description：
 * @modified By：
 * @version: $
 */
public class TableFunctionDemo {
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

        blinkStreamTableEnv.registerFunction("split",new Split("_"));

        //table
        Table select = sensorTable.joinLateral("split(id) as (word , length)").select("id,ts,word,length");

        //sql
        Table table = blinkStreamTableEnv.sqlQuery("select id,ts,word,length " +
            " from sensorTable, lateral table(split(id)) as splitid(word,length)");

        blinkStreamTableEnv.toAppendStream(select, Row.class).print("sql");
        blinkStreamTableEnv.toAppendStream(table, Row.class).print("table");

        env.execute();

    }

    public static class Split extends TableFunction<Tuple2<String,Integer>>{

        private String separaotr ;

        public Split(String separaotr) {
            this.separaotr = separaotr;
        }


        public void eval(String str) {
            String[] s = str.split("_");
            collect(new Tuple2<String,Integer>(s[0],Integer.valueOf(s[1])));
        }

    }
}
