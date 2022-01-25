package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink21_SQL_GroupWindow_SessionWindow {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.创建以文件为数据源表
        tableEnv.executeSql("create table sensor(" +
                "id string," +
                "ts bigint," +
                "vc int, " +
                "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                "watermark for t as t - interval '5' second)" +
                "with("
                + "'connector' = 'filesystem',"
                + "'path' = 'input/sensor-sql.txt',"
                + "'format' = 'csv'"
                + ")"
        );

        //TODO 4.开启一个基于事件时间的会话窗口
        tableEnv.executeSql("SELECT id, " +
                "  session_START(t, INTERVAL '2' second) as wStart,  " +
                "  session_END(t, INTERVAL '2' second) as wEnd,  " +
                "  SUM(vc) sum_vc " +
                "FROM sensor " +
                "GROUP BY session(t, INTERVAL '2' second), id"
        ).print();
    }
}
