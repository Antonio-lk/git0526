package com.atguigu.day03.sink;

import com.atguigu.bean.WaterSensor;
import com.mysql.jdbc.Driver;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink05_Sink_JDBC {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 34567);

        //3.将数据转为JSON格式的字符串
        SingleOutputStreamOperator<WaterSensor> result = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));

                return waterSensor;
            }
        });

        //TODO 4.JDBCSink将数据写入MySQL
        result.addSink(JdbcSink.sink(
                "insert into sensor values (?,?,?)",
                (ps,t)->{
                    ps.setString(1,t.getId());
                    ps.setLong(2, t.getTs());
                    ps.setInt(3, t.getVc());
                },
                new JdbcExecutionOptions.Builder()
                        //设置来一条写一条与ES中相似
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop102:3306/flink_sink_test?useSSL=false")
                        .withUsername("root")
                        .withPassword("123456")
                        .withDriverName(Driver.class.getName())
                        .build()
        ));


        env.execute();

    }
}
