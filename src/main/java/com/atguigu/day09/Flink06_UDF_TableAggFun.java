package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.In;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink06_UDF_TableAggFun {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口获取数据
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env.socketTextStream("hadoop102", 39999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });

        //3.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //将流转为表
        Table table = tableEnv.fromDataStream(waterSensorStream);

        //不注册直接使用
   /*     table
                .groupBy($("id"))
                .flatAggregate(call(MyUDTAF.class, $("vc")).as("value", "top"))
                .select($("id"),$("value"),$("top"))
                .execute()
                .print(); */
        //先注册再使用
        tableEnv.createTemporarySystemFunction("top2", MyUDTAF.class);

        table
                .groupBy($("id"))
                .flatAggregate(call("top2", $("vc")).as("value", "top"))
                .select($("id"), $("value"), $("top"))
                .execute()
                .print();
    }

    //TODO 自定义一个类继承TableAggregateFunction抽象类，通过输入的vc，返回排名前二（倒序）
    //创建一个累加器
    public static class MyAcc {
        public Integer first;
        public Integer second;
    }

    public static class MyUDTAF extends TableAggregateFunction<Tuple2<Integer, Integer>, MyAcc> {

        /**
         * 初始化累加器
         *
         * @return
         */
        @Override
        public MyAcc createAccumulator() {
            MyAcc acc = new MyAcc();
            acc.first = Integer.MIN_VALUE;
            acc.second = Integer.MIN_VALUE;
            return acc;
        }

        /**
         * 累加操作
         *
         * @param acc
         * @param value
         */
        public void accumulate(MyAcc acc, Integer value) {
//            if (value > acc.first) {
//                //先将之前的第一保存为第二名
//                acc.second = acc.first;
//                //当前值大于之前所保存的第一名的值
//                acc.first = value;
//            } else if (value > acc.second) {
//                //将当前值替换为第二名
//                acc.second = value;
//            }
            if (value > acc.first) {
                acc.second = acc.first;
                acc.first = value;
            } else if (value > acc.second) {
                acc.second = value;
            }

        }

        /**
         * 返回最终结果
         *
         * @param acc
         * @param out
         */
        public void emitValue(MyAcc acc, Collector<Tuple2<Integer, Integer>> out) {
//            if (acc.first != Integer.MIN_VALUE) {
//                out.collect(Tuple2.of(acc.first, 1));
//            }
//
//            if (acc.second != Integer.MIN_VALUE) {
//                out.collect(Tuple2.of(acc.second, 2));
//            }
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first,1));
            }

            if (acc.second != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second,2));
            }

        }
    }

}
