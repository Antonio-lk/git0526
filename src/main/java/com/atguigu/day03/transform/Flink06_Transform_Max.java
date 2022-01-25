package com.atguigu.day03.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink06_Transform_Max {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.对数据做map操作
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //4.将数据按照相同的id聚合
//        waterSensorSingleOutputStreamOperator.keyBy(WaterSensor::getId)
        KeyedStream<WaterSensor, String> keyedStream = waterSensorSingleOutputStreamOperator.keyBy(r -> r.getId());

        //5.max聚合操作，求vc的最大值
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.max("vc");
        SingleOutputStreamOperator<WaterSensor> maxByResult = keyedStream.maxBy("vc", true);
        SingleOutputStreamOperator<WaterSensor> maxByResult2 = keyedStream.maxBy("vc", false);

//        result.print("max:");

        maxByResult.print("true:");
        maxByResult2.print("false:");

        env.execute();

    }
}
