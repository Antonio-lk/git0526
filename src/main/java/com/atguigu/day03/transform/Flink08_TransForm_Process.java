package com.atguigu.day03.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

class Flink08_TransForm_Process {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 34567);

        //3.使用process对数据做map操作
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = streamSource.process(new ProcessFunction<String, WaterSensor>() {
            @Override
            public void processElement(String value, Context ctx, Collector<WaterSensor> out) throws Exception {
                String[] split = value.split(",");
                out.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
            }
        });


        //4.将数据按照相同的id聚合
//        waterSensorSingleOutputStreamOperator.keyBy(WaterSensor::getId)
        KeyedStream<WaterSensor, String> keyedStream = waterSensorSingleOutputStreamOperator.keyBy(r -> r.getId());

        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, Integer>() {
            //创建一个累加器，用来统计个数
            private Integer count = 0;

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<Integer> out) throws Exception {
                System.out.println("processElement...");
                count++;
                out.collect(count);
            }
        }).print("数据个数");

        waterSensorSingleOutputStreamOperator.print("经过process将字符串转为JavaBean：");

        env.execute();
    }
}
