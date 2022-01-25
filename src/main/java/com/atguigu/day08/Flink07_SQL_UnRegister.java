package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.e;


public class Flink07_SQL_UnRegister {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境，获取有数据的流对象
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));


        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //将流转为动态表 未注册的表
        Table table = tableEnv.fromDataStream(waterSensorStream);

        //TODO 写sql查询数据
//        Table resultTable = tableEnv.sqlQuery("select * from " + table + " where id='sensor_1'");

        //打印数据方式一：
//        tableEnv.toAppendStream(resultTable, Row.class).print();
//
//        env.execute();

        //打印数据方式二
//        resultTable.execute().print();

        //打印数据方式三：
        tableEnv.executeSql("select * from " + table + " where id='sensor_1'").print();

    }
}