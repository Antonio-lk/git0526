package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Flink02_TableAPI_Demo_Agg {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //2.获取数据
        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));
        //TODO 3.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 4.将流转为动态表
        Table table = tableEnv.fromDataStream(waterSensorStream);

        //TODO 5.查询表中内容 聚合操作
        /*Table resultTable = table
                .groupBy($("id"))
                .aggregate($("vc").sum().as("vcSum"))
                .select($("id"), $("vcSum"));*/
        Table resultTable = table
                .groupBy($("id"))
                .select($("id"), $("vc").sum());

        //TODO 6.将结果表转为流
        //追加流
//        DataStream<Row> result = tableEnv.toAppendStream(resultTable, Row.class);
        //撤回流
        DataStream<Tuple2<Boolean, Row>> result = tableEnv.toRetractStream(resultTable, Row.class);


        result.print();

        env.execute();

    }
}
