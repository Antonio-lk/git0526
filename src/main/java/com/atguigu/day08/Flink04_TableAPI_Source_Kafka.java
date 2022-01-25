package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Flink04_TableAPI_Source_Kafka {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //TODO 3.连接外部文件系统，将文件中的数据映射到表中
        Schema schema = new Schema();
        schema.field("id", DataTypes.STRING());
        schema.field("ts", DataTypes.BIGINT());
        schema.field("vc", DataTypes.INT());

        tableEnv
                .connect(new Kafka()
                        .version("universal")
                        .topic("sensor")
                        .startFromLatest()
                        .property("group.id", "bigdata")
                        .property("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
                )
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        //TODO 4.将动态表转为表对象
        Table sensor = tableEnv.from("sensor");

        Table resultTable = sensor
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));

//        Table resultTable = tableEnv.sqlQuery("select * from sensor where id='sensor_1'");

        //将表转为流
        tableEnv.toAppendStream(resultTable, Row.class).print();

        env.execute();
    }
}
