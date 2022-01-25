package com.atguigu.day03.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink09_Transform_Repartition {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //2.对数据做map操作
        SingleOutputStreamOperator<String> map = streamSource.map(r -> r).setParallelism(2);

        KeyedStream<String, String> keyedStream = map.keyBy(r -> r);
        DataStream<String> shuffle = map.shuffle();
        DataStream<String> rebalance = map.rebalance();
        DataStream<String> rescale = map.rescale();
        DataStream<String> forward = map.forward();

        map.print("原始数据：").setParallelism(2);
//        keyedStream.print("keyBy:");
//        shuffle.print("shuffle:");
//        rebalance.print("rebalance:");
//        rescale.print("rescale:");
        forward.print("forward").setParallelism(2);


        env.execute();
    }
}
