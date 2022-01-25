package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Flink08_WaterMark_EventTime_Timer {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 39999);

        //3.将数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        SingleOutputStreamOperator<WaterSensor> sensorSingleOutputStreamOperator = waterSensorSingleOutputStreamOperator.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs() * 1000;
                            }
                        })
        );


        KeyedStream<WaterSensor, String> keyedStream = sensorSingleOutputStreamOperator.keyBy(r -> r.getId());

        //TODO 使用基于事件时间的定时器
        SingleOutputStreamOperator<String> result = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {


            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //注册一个时间为5S的基于事件时间的定时器
                System.out.println("注册一个定时器"+ctx.getCurrentKey());
                ctx.timerService().registerEventTimeTimer(ctx.timestamp()+5000);

            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                out.collect("定时器被触发。。。。。"+ctx.getCurrentKey());

            }
        });

        result.print();

        env.execute();
    }
}
