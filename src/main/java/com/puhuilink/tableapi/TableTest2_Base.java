package com.puhuilink.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/6 17:43
 * @description：
 * @modified By：
 * @version: $
 */
public class TableTest2_Base {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env);

        //2.表的创建：连接外部系统，读取数据
        //2.1 读取数据
        blinkStreamTableEnv.connect(new FileSystem().path("C:\\Users\\无敌大大帅逼\\IdeaProjects\\flink-test\\flinkdemo\\src\\main\\resources\\Sensor.csv"))
            .withFormat(new Csv().fieldDelimiter(','))
            .withSchema(new Schema().field("id", DataTypes.STRING())
                                    .field("timestamp",DataTypes.BIGINT())
                                    .field("temp",DataTypes.DOUBLE()))
            .createTemporaryTable("inputTable");


        Table table = blinkStreamTableEnv.from("inputTable");
//        table.printSchema();
//        blinkStreamTableEnv.toAppendStream(table,Row.class).print();

        //3.查询转换
        //3.1 Table API
        //简单转换
        table.select("id,temp").filter("id === 'sensor_6'");

        //聚合统计
        Table aggTable = table.groupBy("id").select("id,id.count as count ,temp.avg as avgTemp");

        //3.2 SQL
        Table sqlTable = blinkStreamTableEnv.sqlQuery("select id, temp from inputTable where id = 'sensor_6'");

        blinkStreamTableEnv.toRetractStream(aggTable,Row.class).print("aggTable");
        blinkStreamTableEnv.toAppendStream(sqlTable,Row.class).print("sqlTable");

        env.execute();
    }
}
