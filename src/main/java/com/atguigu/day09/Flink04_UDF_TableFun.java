package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;


import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink04_UDF_TableFun {
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
    /*    table
                .joinLateral(call(MyUDTF.class, $("id")))
                .select($("id"),$("word"),$("num"))
                .execute()
                .print();*/
        //先注册再使用
        tableEnv.createTemporarySystemFunction("MyUdtf", MyUdtf.class);

        //TableAPI
       /* table
                .joinLateral(call("MyUDTF", $("id")))
                .select($("id"),$("word"),$("num"))
                .execute()
                .print();*/
        //SQL
//        tableEnv.executeSql("select id,word,num from "+table+" join lateral table(MyUDTF(id)) on true").print();
        tableEnv.executeSql("select id,wd,nm from "+table+",lateral table(MyUdtf(id))").print();
    }

    //TODO 自定义一个类继承TableFunction抽象类，通过输入的id，根据下划线炸出数据
    //hint暗示，主要作用为类型推断时使用
    @FunctionHint(output = @DataTypeHint("Row<word String,num Integer>"))
    public static class MyUDTF extends TableFunction<Row> {

        public void eval(String value) {
            String[] split = value.split("_");
            collect(Row.of(split[0], Integer.parseInt(split[1])));
        }
    }

    @FunctionHint(output = @DataTypeHint("Row<wd String,nm Integer>"))
    public static class MyUdtf extends TableFunction<Row> {

        public void eval(String value) {
            String[] split = value.split("_");
            collect(Row.of(split[0],Integer.parseInt(split[1])));
        }
    }

}

