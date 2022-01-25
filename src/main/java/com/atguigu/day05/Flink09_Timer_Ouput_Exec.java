package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink09_Timer_Ouput_Exec {
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

        KeyedStream<WaterSensor, String> keyedStream = waterSensorSingleOutputStreamOperator.keyBy(r -> r.getId());

        //TODO 监控水位传感器的水位值，如果水位值在五秒钟之内连续上升，则报警，并将报警信息输出到侧输出流。
        SingleOutputStreamOperator<String> result = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            //上一次的水位值
            private Integer lastVc = Integer.MIN_VALUE;

            //定时器时间
            private Long timer = Long.MIN_VALUE;

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //判断当前水位与上一次水位比是否上升
                if (value.getVc()>lastVc){
                    if (timer==Long.MIN_VALUE){
                        //证明定时器没被注册过
                        //注册一个定时器
                        System.out.println("注册一个定时器"+ctx.getCurrentKey());
                        timer=ctx.timerService().currentProcessingTime() + 5000;
                        ctx.timerService().registerProcessingTimeTimer(timer);
                    }
                }else {
                    System.out.println("删除一个定时器"+ctx.getCurrentKey());
                    ctx.timerService().deleteProcessingTimeTimer(timer);
                    //重置定时器时间
                    timer = Long.MIN_VALUE;
                }

                //更新水位
                lastVc = value.getVc();
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

                //定时器触发之后
                ctx.output(new OutputTag<String>("output"){}, "报警！！！！水位连续上升");

                //重置定时器时间
                timer = Long.MIN_VALUE;
            }
        });

        result.print("主流");
        result.getSideOutput(new OutputTag<String>("output"){}).print("侧输出");

        env.execute();
    }
}
