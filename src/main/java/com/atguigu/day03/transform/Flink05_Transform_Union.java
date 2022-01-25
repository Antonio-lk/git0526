package com.atguigu.day03.transform;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class Flink05_Transform_Union {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        DataStreamSource<String> integerDataStreamSource = env.fromElements("1", "2", "3", "4", "5");

        DataStreamSource<String> stringDataStreamSource = env.fromElements("a", "b", "c", "d", "e");

        //union连接两条流
        DataStream<String> union = integerDataStreamSource.union(stringDataStreamSource);

        //对union之后的流做处理
        union.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                out.collect(value + "1");
            }
        }).print();

        env.execute();

    }
}
