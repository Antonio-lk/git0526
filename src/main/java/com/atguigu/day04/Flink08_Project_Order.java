package com.atguigu.day04;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class Flink08_Project_Order {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从文件读取数据
        DataStreamSource<String> orderStream = env.readTextFile("input/OrderLog.csv");

        DataStreamSource<String> txStream = env.readTextFile("input/ReceiptLog.csv");

        //3.将数据转为JavaBean
        SingleOutputStreamOperator<OrderEvent> orderEventStream = orderStream.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new OrderEvent(
                        Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3])
                );
            }
        });

        SingleOutputStreamOperator<TxEvent> txEventStream = txStream.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new TxEvent(
                        split[0],
                        split[1],
                        Long.parseLong(split[2])
                );
            }
        });

        //4.连接两条流
        ConnectedStreams<OrderEvent, TxEvent> connect = orderEventStream.connect(txEventStream);

        //5.将相同连接条件的数据聚和到一块
        ConnectedStreams<OrderEvent, TxEvent> orderEventTxEventConnectedStreams = connect.keyBy("txId", "txId");

        //6.关联两条流的数据
        orderEventTxEventConnectedStreams.process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {

            HashMap<String, OrderEvent> orderEventHashMap = new HashMap<>();

            HashMap<String, TxEvent> txMap = new HashMap<>();

            @Override
            public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                //处理订单表的数据
                if (txMap.containsKey(value.getTxId())) {
                    //有能关联上的数据
                    out.collect("订单:" + value.getOrderId() + "对账成功！！！");
                    //删除已经关联上的数据
                    txMap.remove(value.getTxId());
                } else {
                    //关联不上,将自己存入缓存
                    orderEventHashMap.put(value.getTxId(), value);
                }
            }

            @Override
            public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                //处理交易表数据
                if (orderEventHashMap.containsKey(value.getTxId())) {
                    out.collect("订单:" + orderEventHashMap.get(value.getTxId()).getOrderId() + "对账成功！！！");
                    orderEventHashMap.remove(value.getTxId());
                } else {
                    txMap.put(value.getTxId(), value);
                }

            }
        }).print();

        env.execute();
    }
}
