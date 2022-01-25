package com.atguigu.day04;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Flink16_Window_Fun_Agg {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 39999);

        //3.将数据转为Tuple元组
        SingleOutputStreamOperator<WaterSensor> wordToOneStream = streamSource.flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String value, Collector<WaterSensor> out) throws Exception {
                String[] split = value.split(",");
                out.collect(new WaterSensor(split[0],Long.parseLong(split[1])*1000,Integer.parseInt(split[2])));
            }
        });

        //4.将相同单词的数据聚和到一块
        KeyedStream<WaterSensor, Tuple> keyedStream = wordToOneStream.keyBy("id");

        //5.开启一个基于时间的滚动窗口
        WindowedStream<WaterSensor, Tuple, TimeWindow> window = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        //TODO 使用窗口函数，增量聚合函数Aggregate，对数据做累加操作
        window.aggregate(new AggregateFunction<WaterSensor, Integer, Integer>() {
            /**
             * 初始化累加器 =》创建累加器
             *
             * @return
             */
            @Override
            public Integer createAccumulator() {
                System.out.println("初始化累加器。。。");
                return 0;
            }

            /**
             * 累加操作(累加指的是累加累加器的值)
             *
             * @param value
             * @param accumulator
             * @return
             */
            @Override
            public Integer add(WaterSensor value, Integer accumulator) {
                System.out.println("累加操作。。。");
                return accumulator + value.getVc();
            }

            /**
             * 返回结果
             *
             * @param accumulator
             * @return
             */
            @Override
            public Integer getResult(Integer accumulator) {
                System.out.println("返回结果。。。");
                return accumulator;
            }

            /**
             * 只用于会话窗口
             *
             * @param a
             * @param b
             * @return
             */
            @Override
            public Integer merge(Integer a, Integer b) {
                System.out.println("合并。。。");
                return a + b;
            }
        }).print();


        env.execute();
    }
}
