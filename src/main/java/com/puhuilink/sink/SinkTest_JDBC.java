package com.puhuilink.sink;

import com.puhuilink.pojo.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/1 17:34
 * @description：
 * @modified By：
 * @version: $
 */
public class SinkTest_JDBC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        DataStream<String> stringDataStream = executionEnvironment.readTextFile("C:\\Users\\无敌大大帅逼\\IdeaProjects\\flink-test\\flinkdemo\\src\\main\\resources\\Sensor.csv");
        SingleOutputStreamOperator<SensorReading> map = stringDataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
            }
        });

        map.addSink(new MyJdbcSink());

        executionEnvironment.execute();
    }

    private static class MyJdbcSink extends org.apache.flink.streaming.api.functions.sink.RichSinkFunction<SensorReading> {

        private Connection connection = null;

        PreparedStatement insertStmt = null;

        PreparedStatement updateStmt = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            connection = DriverManager.getConnection("jdbc:postgresql://192.168.1.224:5432/qbs_demo", "postgres", "postgres123QAZ");
            insertStmt = connection.prepareStatement("insert into sensor_temp (id,temp) values (?,?)");
            updateStmt = connection.prepareStatement("update sensor_temp set temp = ? where id = ?");
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            //每来一条数据，调用连接，执行sql
            //先更新，if 失败 插入 else 更新
            updateStmt.setDouble(1,value.getTemperature());
            updateStmt.setString(2,value.getId());
            updateStmt.execute();
            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1,value.getId());
                insertStmt.setDouble(2,value.getTemperature());
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            connection.close();
            super.close();
        }
    }
}
