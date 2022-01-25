package com.atguigu.day03.sink;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Flink04_Sink_Custom {
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

        //TODO 4.自定义Sink将数据写入MySQL
        result.addSink(new MySink());


        env.execute();
    }
    public static class MySink extends RichSinkFunction<WaterSensor> {
        private Connection connection;

        private PreparedStatement pstm;

        @Override
        public void open(Configuration parameters) throws Exception {
            //1.获取Mysql连接
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/flink_sink_test?useSSL=false",
                    "root", "123456");

            //2.声明sql语句
            pstm = connection.prepareStatement("insert into sensor values (?,?,?)");
        }

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {

            //3.给占位符赋值
            pstm.setString(1, value.getId());
            pstm.setLong(2, value.getTs());
            pstm.setInt(3, value.getVc());

            //4.执行赋值操作
            pstm.execute();

        }

        @Override
        public void close() throws Exception {
            //5.关闭连接
            pstm.close();
            connection.close();
        }

    }
}
