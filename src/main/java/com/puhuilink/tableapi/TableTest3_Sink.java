package com.puhuilink.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/7 10:09
 * @description：
 * @modified By：
 * @version: $
 */
public class TableTest3_Sink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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


        Table sqlTable = blinkStreamTableEnv.sqlQuery("select id, temp from inputTable where id = 'sensor_6'");


        //4 Sink
        //4.1 输出到文件
        blinkStreamTableEnv.connect(new FileSystem().path("C:\\Users\\无敌大大帅逼\\IdeaProjects\\flink-test\\flinkdemo\\src\\main\\resources\\Sensor_Test.csv"))
            .withFormat(new Csv().fieldDelimiter(','))
            .withSchema(new Schema().field("id", DataTypes.STRING())
                .field("temp",DataTypes.DOUBLE()))
            .createTemporaryTable("outputTable");

        sqlTable.insertInto("outputTable");

        env.execute();
    }
}
