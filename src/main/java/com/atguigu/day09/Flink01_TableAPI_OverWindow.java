package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

public class Flink01_TableAPI_OverWindow {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                })
                );

        //获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //将流转为动态表 未注册的表，并指定事件时间字段
//        Table table = tableEnv.fromDataStream(waterSensorSingleOutputStreamOperator, $("id"), $("ts"), $("vc"),
////                $("pt").proctime());
        Table table = tableEnv.fromDataStream(waterSensorSingleOutputStreamOperator,$("id"),$("ts").rowtime(),$("vc"));

        //TODO 使用overWindow进行开窗
        table
                //开启一个基于事件时间的滚动窗口
//                .window(Over.partitionBy($("id")).orderBy($("ts")).as("w"))
                //使用无界的overWindow
//                .window(Over.partitionBy($("id")).orderBy($("pt")).as("w"))
                //使用有界的overWindow(基于时间)
//                .window(Over.partitionBy($("id")).orderBy($("ts")).preceding(lit(2).second()).as("w"))
                //基于行数
                .window(Over.partitionBy($("id")).orderBy($("ts")).preceding(rowInterval(2L)).as("w"))


                .select($("id"),$("ts"),$("vc"),$("vc").sum().over($("w")).as("vcSum"))
                .execute()
                .print();




    }
}
