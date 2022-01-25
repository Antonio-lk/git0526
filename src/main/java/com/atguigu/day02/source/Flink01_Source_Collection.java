package com.atguigu.day02.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class Flink01_Source_Collection {
    public static void main(String[] args) throws Exception {
        //1.创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置全局并行度为1
        env.setParallelism(1);

        //TODO 3.从集合中获取数据
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);

        //并行度只能为1
        DataStreamSource<Integer> streamSource = env.fromCollection(list);

        //TODO 从文件中获取数据
//        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt");

        //TODO 从socket中获取数据
//        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999).setParallelism(2);

        //TODO 从元素中获取数据
//        DataStreamSource<Integer> streamSource = env.fromElements(1, 2, 3, 4);

        streamSource.print();

        env.execute();
    }
}