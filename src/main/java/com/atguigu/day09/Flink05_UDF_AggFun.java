package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink05_UDF_AggFun {
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
      /*  table
                .groupBy($("id"))
                .select($("id"),call(MyUDAF.class,$("vc")))
                .execute()
                .print();*/
        //先注册再使用
        tableEnv.createTemporarySystemFunction("MyUDAF",MyUDAF.class);

        //TableAPI
/*        table
                .groupBy($("id"))
                .select($("id"),call("MyUDAF", $("vc")))
                .execute()
                .print();*/
        //SQL0
        tableEnv.executeSql("select id,MyUDAF(vc) from "+table+" group by id").print();

    }

    //TODO 自定义一个类继承AggregateFunction抽象类，通过输入的vc，返回最大值

    public static class MyACC{
        public Integer max = Integer.MIN_VALUE;
    }

    public static class MyUDAF extends AggregateFunction<Integer, Tuple1<Integer>>{

        @Override
        public Tuple1<Integer> createAccumulator() {
            return Tuple1.of(Integer.MIN_VALUE);
        }

        public void accumulate(Tuple1<Integer> acc,Integer value){
            acc.f0 = Math.max(acc.f0, value);
        }

        @Override
        public Integer getValue(Tuple1<Integer> accumulator) {
            return accumulator.f0;
        }
    }
}
