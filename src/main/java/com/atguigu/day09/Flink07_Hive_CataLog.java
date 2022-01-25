package com.atguigu.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class Flink07_Hive_CataLog {
    public static void main(String[] args) {
        //1.流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.setProperty("HADOOP_USER_NAME", "atguigu");

        env.setParallelism(1);

        //2.表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String name            = "myhive";  // Catalog 名字
        String defaultDatabase = "flink_test"; // 默认数据库
        String hiveConfDir     = "c:/conf"; // hive配置文件的目录. 需要把hive-site.xml添加到该目录


        //3.创建HiveCatalog
        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir);

        //4.注册hiveCataLog
        tableEnv.registerCatalog(name, hiveCatalog);

        tableEnv.useCatalog(name);
        tableEnv.useDatabase(defaultDatabase);

        //设置hive方言
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        //5.写sql查询hive中数据
        tableEnv.executeSql("select * from stu").print();
    }
}