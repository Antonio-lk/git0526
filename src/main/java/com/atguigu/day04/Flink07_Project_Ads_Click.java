package com.atguigu.day04;

import com.atguigu.bean.AdsClickLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink07_Project_Ads_Click {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/AdClickLog.csv");

        //3.将字符串组成JavaBean
        SingleOutputStreamOperator<AdsClickLog> adsClickLogStream = streamSource.map(new MapFunction<String, AdsClickLog>() {
            @Override
            public AdsClickLog map(String value) throws Exception {
                String[] split = value.split(",");
                return new AdsClickLog(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        split[2],
                        split[3],
                        Long.parseLong(split[4])
                );
            }
        });


        //4.将数据组成JavaBean
        adsClickLogStream.map(new MapFunction<AdsClickLog, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(AdsClickLog value) throws Exception {
                return Tuple2.of(value.getProvince() + "-" + value.getAdId(), 1);
            }
        })
                .keyBy(0)
                .sum(1)
                .print();

        env.execute();
    }
}
