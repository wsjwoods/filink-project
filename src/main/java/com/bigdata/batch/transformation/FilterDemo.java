package com.bigdata.batch.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class FilterDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> stringDataSource = env.readTextFile("E:\\filink-project\\src\\main\\resources\\wordcount");

//        stringDataSource.map(new MapFunction<String, Tuple2<String,Integer>>() {
//            @Override
//            public Tuple2<String,Integer> map(String s) throws Exception {
//
//                //因为map是一对一的，也就是说，一条数据，只能返回一行
//                String[] split = s.split(" ");
//                for (String t:split) {
//                    return new Tuple2<>(t,1);
//                }
//                return new Tuple2<>(s,1);
//            }
//        }).print();

        DataSet<Tuple2<String, Integer>> mapPartitionOperator = stringDataSource.mapPartition(new MapPartitionFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void mapPartition(Iterable<String> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : iterable) {
                    for (String s : word.split(" ")) {
                        collector.collect(new Tuple2<>(s, 1));
                    }
                }
            }
        });

        mapPartitionOperator.filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> t) throws Exception {
                return t.f0.startsWith("f");
            }
        });

        mapPartitionOperator.map(new MapFunction<Tuple2<String,Integer>, Object>() {
            @Override
            public Object map(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2<>(t.f1,t.f0);
            }
        });

        //通过project可以非常方便的选择你想留下的元素
        mapPartitionOperator.project(1).print();
    }
}
