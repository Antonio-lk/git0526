package com.atguigu.day06;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class Flink08_State_Operator_3 {
    public static void main(String[] args) throws Exception {
        //1.流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从本地获取数据流
        DataStreamSource<String> localStream = env.socketTextStream("hadoop102", 29999);
        //从虚拟机获取数据量
        DataStreamSource<String> hadoopStream = env.socketTextStream("hadoop102", 39999);

        //定义状态并广播
        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("map-State", String.class, String.class);

        BroadcastStream<String> broadcastStream = hadoopStream.broadcast(mapStateDescriptor);

        //连接两条流
        BroadcastConnectedStream<String, String> connect = localStream.connect(broadcastStream);

        connect.process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                //获取到广播状态中的数据
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                String s = broadcastState.get("switch");

                if ("1".equals(s)) {
                    out.collect("执行逻辑1.。。。。");
                } else if ("2".equals(s)) {
                    out.collect("执行逻辑2.。。。。");
                } else {
                    out.collect("执行其他逻辑。。。。");
                }

            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {

                //提取状态
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

                //将数据存入状态
                broadcastState.put("switch", value);
            }
        }).print();

        env.execute();
    }
}
