package com.atguigu.day06;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink10_CheckPoint_SavePoint {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置用户权限
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink0526/ck"));

        //开启CK
        // 每 5000ms 开始一次 checkpoint
        env.enableCheckpointing(5000);
        // 设置模式为精确一次 (这是默认值)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //2.读取端口数据并转换为元组
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOneDStream = env.socketTextStream("hadoop102", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            out.collect(Tuple2.of(word, 1L));
                        }
                    }
                });
        //3.按照单词分组
        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordToOneDStream.keyBy(r -> r.f0);

        //4.累加计算
        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyedStream.sum(1);

        //5.打印
        result.print();

        //6.开启任务
        env.execute();

    }

}