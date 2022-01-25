package com.atguigu.day03.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink03_Transform_Shuffle {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

//        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt");
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //提取每个单词
        SingleOutputStreamOperator<String> flatMapStream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        }).setParallelism(2);

        DataStream<String> shuffle = flatMapStream.shuffle();

        flatMapStream.print("原始数据").setParallelism(2);
        shuffle.print("shuffle之后");

        env.execute();


    }
}
