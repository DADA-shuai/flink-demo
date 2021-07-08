package com.puhuilink.tableapi;

import com.puhuilink.pojo.Metric;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/7 10:19
 * @description：
 * @modified By：
 * @version: $
 */
public class TableTest4_KafkaPipeLine {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env,blinkStreamSettings);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","cdh1:9092,cdh2:9092,cdh3:9092");
        properties.setProperty("zookeeper.connect","cdh1:2181,cdh2:2181,cdh3:2181");
        properties.setProperty("group.id","flink");

        blinkStreamTableEnv.connect(new Kafka()
            .version("0.11")
            .topic("test6")
            .properties(properties)
        )
            .withFormat(new Json())
            .withSchema(new Schema()
                .field("host_id", DataTypes.BIGINT())
                .field("endpoint_id", DataTypes.BIGINT())
                .field("metric_id", DataTypes.BIGINT())
                .field("metric_value", DataTypes.DOUBLE())
                .field("collect_time", DataTypes.BIGINT())
                .field("agent", DataTypes.STRING())
                .field("aggregate", DataTypes.BOOLEAN())
            )
            .createTemporaryTable("kafkaTable");

        Table kafkaTable = blinkStreamTableEnv.from("kafkaTable");

        blinkStreamTableEnv.toAppendStream(kafkaTable,Row.class).print();

        blinkStreamTableEnv.connect(new Kafka()
            .version("0.11")
            .topic("test")
            .properties(properties)
        )
            .withFormat(new Json())
            .withSchema(new Schema()
                .field("host_id", DataTypes.BIGINT())
                .field("endpoint_id", DataTypes.BIGINT())
                .field("metric_id", DataTypes.BIGINT())
                .field("metric_value", DataTypes.DOUBLE())
                .field("collect_time", DataTypes.BIGINT())
                .field("agent", DataTypes.STRING())
                .field("aggregate", DataTypes.BOOLEAN())
            )
            .createTemporaryTable("outputTable");

        kafkaTable.insertInto("outputTable");

//        String sinkDDL = "create table outputTable\n" +
//            "(\n" +
//            "    host_id          bigint,\n" +
//            "    endpoint_id      bigint,\n" +
//            "    metric_id        bigint,\n" +
//            "    metric_value     double precision,\n" +
//            "    collect_time     bigint,\n" +
//            "    agent            varchar(20),\n" +
//            "    aggregate        boolean\n" +
//            ") with (\n" +
//            "    'connector.type' = 'jdbc',\n" +
//            "    'connector.url' = 'jdbc:postgresql://192.168.1.224:5432/qbs_demo',\n" +
//            "    'connector.table' = 't_metric',\n" +
//            "    'connector.driver' = 'com.postgresql.Driver',\n" +
//            "    'connector.username' = 'postgres',\n" +
//            "    'connector.password' = 'postgres123QAZ'\n" +
//            ")";
//        blinkStreamTableEnv.sqlUpdate(sinkDDL);
//        kafkaTable.insertInto("outputTable");

        blinkStreamTableEnv.toAppendStream(kafkaTable, Row.class).print();


        env.execute();

    }
}
