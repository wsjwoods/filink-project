package com.bigdata.batch.transformation;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class MapAndReduceMu {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> stringDataSource = env.readTextFile("E:\\filink-project\\src\\main\\resources\\wordcount");

        //一般来说，程序优化的时候，可以在这里进行
        DataSet<Tuple2<String,Integer>> mapPartitionStr = stringDataSource.mapPartition(new MapPartitionFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void mapPartition(Iterable<String> iterable, Collector<Tuple2<String,Integer>> collector) throws Exception {
                for (String word : iterable) {
                    for (String s : word.split(" ")) {
                        collector.collect(new Tuple2<>(s, 1));
                    }
                }
            }
        });
//这是对reduce操作的一个小优化
        mapPartitionStr.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<String,Integer>, Object>() {
            @Override
            public void reduce(Iterable<Tuple2<String, Integer>> iterable, Collector<Object> collector) throws Exception {
                String key = null;
                int count = 0;

                for (Tuple2<String, Integer> t:iterable) {
                    key = t.f0;
                    count = count+t.f1;
                }
                collector.collect(new Tuple2<>(key,count));
            }
        });
//进一步的reduce优化   如果我们事先在每一台机器上先进行相加操作，那么汇总机器的数据量少很多，计算速度也会变快
        DataSet<Tuple2<String, Integer>> tuple2ObjectGroupCombineOperator = mapPartitionStr.groupBy(0).combineGroup(new GroupCombineFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void combine(Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String key = null;
                int count = 0;

                for (Tuple2<String, Integer> t : iterable) {
                    key = t.f0;
                    count = count + t.f1;
                }
                collector.collect(new Tuple2<>(key, count));
            }
        });

        //在每天机器上combin之后的结果只是每台机器的结果，最终还是要将结果汇总
        tuple2ObjectGroupCombineOperator.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<String,Integer>, Object>() {
            @Override
            public void reduce(Iterable<Tuple2<String, Integer>> iterable, Collector<Object> collector) throws Exception {
                String key = null;
                int count = 0;

                for (Tuple2<String, Integer> t:iterable) {
                    key = t.f0;
                    count = count+t.f1;
                }
                collector.collect(new Tuple2<>(key,count));
            }
        }).print();


    }
}
