package com.puhuilink.tableapi;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/6 17:07
 * @description：
 * @modified By：
 * @version: $
 */
public class TableTest1_Common {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //1.1基于老版本的planner的流处理
        EnvironmentSettings oldStreamSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, oldStreamSettings);

        //1.2基于老版本的批处理
        EnvironmentSettings oldBatchSettings = EnvironmentSettings.newInstance().useOldPlanner().inBatchMode().build();
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(batchEnv);

        //1.3基于Blink的流处理
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings);

        //1.4基于Blink的批处理
        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment blinkTableEnvironment = TableEnvironment.create(blinkBatchSettings);


        env.execute();
    }
}
