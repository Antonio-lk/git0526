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


public class Flink06_TableAPI_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境，获取数据并创建流对象
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
        //流环境=>表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //将流转为动态表
        //通过表环境，将流对象=>表对象
        Table table = tableEnv.fromDataStream(waterSensorStream);
        //显然，表对象可以直接调方法查询，并返回新表对象
        //表环境也可以直接调sqlQuery写sql进行查询，并返回一个表对象
        Table resultTable = table
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));
        //这里没有创建动态表，没有表可以去拿数据，不要以为随便来一个表对象就可以拿了
//        Table resultTable = tableEnv.sqlQuery("select * from table where id='sensor_1'");


        //TODO 3.连接外部文件系统，将文件中的数据映射到表中
        Schema schema = new Schema();
        schema.field("id", DataTypes.STRING());
        schema.field("ts", DataTypes.BIGINT());
        schema.field("vc", DataTypes.INT());

        tableEnv
                .connect(new Kafka()
                        .version("universal")
                        .topic("sensor")
                        .sinkPartitionerRoundRobin()
                        .property("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
                )
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        //TODO 4.将动态表转为表对象
        resultTable.executeInsert("sensor");

    }
}
