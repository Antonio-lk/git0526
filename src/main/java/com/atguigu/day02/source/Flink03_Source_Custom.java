package com.atguigu.day02.source;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class Flink03_Source_Custom {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.自定义source
//        DataStreamSource<WaterSensor> streamSource = env.addSource(new MySource()).setParallelism(2);

        DataStreamSource<WaterSensor> secondSource = env.addSource(new SecondSource()).setParallelism(2);

        secondSource.print();


//        streamSource.print();

        env.execute();
    }

//    public static class MySource implements SourceFunction<WaterSensor> {
    //如果想对Source设置多并行度，需要实现ParallelSourceFunction接口
    public static class MySource implements ParallelSourceFunction<WaterSensor> {
       private Random random = new Random();
        private Boolean isRunning=true;
        /**
         * 将相发送的数据写入run方法中
         * @param ctx
         * @throws Exception
         */
        @Override

        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            while (isRunning) {
                ctx.collect(new WaterSensor("sensor"+random.nextInt(100), System.currentTimeMillis(), random.nextInt(1000)));
            }

        }

        /**
         *终止while循环系统内部调用
         */
        @Override
        public void cancel() {
            isRunning = false;
        }
    }



    public static class SecondSource implements ParallelSourceFunction<WaterSensor> {
        private Random random = new Random();
        private Boolean isRunning=true;

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            while (isRunning){
                ctx.collect(new WaterSensor("sensor"+random.nextInt(100),System.currentTimeMillis(),random.nextInt(1000)));
            }

        }

        @Override
        public void cancel() {
            isRunning=false;
        }
    }
}
