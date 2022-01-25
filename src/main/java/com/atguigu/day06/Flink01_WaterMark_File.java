package com.atguigu.day06;

import com.atguigu.bean.AdsClickLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Flink01_WaterMark_File {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("input/AdClickLog.csv");

        SingleOutputStreamOperator<AdsClickLog> map = streamSource.map(new MapFunction<String, AdsClickLog>() {
            @Override
            public AdsClickLog map(String value) throws Exception {
                String[] split = value.split(",");
                return new AdsClickLog(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        split[2],
                        split[3],
                        Long.parseLong(split[4]));
            }
        });

        SingleOutputStreamOperator<AdsClickLog> streamOperator = map.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<AdsClickLog>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<AdsClickLog>() {
                            @Override
                            public long extractTimestamp(AdsClickLog element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        })
        );

        KeyedStream<AdsClickLog, Tuple> keyedStream = streamOperator.keyBy("userId");

        WindowedStream<AdsClickLog, Tuple, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.milliseconds(20)));


        window.process(new ProcessWindowFunction<AdsClickLog, String, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<AdsClickLog> elements, Collector<String> out) throws Exception {

                System.out.println(context.currentWatermark());
            }
        });


        env.execute();
    }
}
