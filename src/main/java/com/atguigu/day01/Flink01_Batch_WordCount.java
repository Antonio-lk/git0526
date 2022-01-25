package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class Flink01_Batch_WordCount {
    public static void main(String[] args) throws Exception {
        //1.批执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.读取文件中数据
        DataSource<String> dataSource = env.readTextFile("input/word.txt");

        //3.使用flatmap对数据按照空格切分，并组成Tuple二元组(word,1)
        FlatMapOperator<String, Tuple2<String, Integer>> wordToOne = dataSource.flatMap(new MyFlatMap());

        //4.将相同单词的数据聚和到一块
        UnsortedGrouping<Tuple2<String, Integer>> tuple2UnsortedGrouping = wordToOne.groupBy(0);

        //5.计算单词的个数
        AggregateOperator<Tuple2<String, Integer>> result = tuple2UnsortedGrouping.sum(1);

        //6.打印
        result.print();
    }

    //自定义类实现FlatMapFunction接口
    public static class MyFlatMap implements FlatMapFunction<String, Tuple2<String,Integer>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //1.首先按照空格切分
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
//                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
