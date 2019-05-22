package com.bigdata.batch.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class FlatMapFunctionDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> stringDataSource = env.readTextFile("E:\\filink-project\\src\\main\\resources\\wordcount");

        DataSet<String> flatmapResult = stringDataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] split = s.split(" ");
                for (String word : split) {
                    collector.collect(word);
                }
            }
        });

        flatmapResult.map(new MapFunction<String,Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String,Integer> map(String o) throws Exception {
                return new Tuple2<>(o,1);
            }
        }).groupBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return new Tuple2<String, Integer>(t1.f0,t1.f1+t2.f1);
            }
        }).print();
//                .groupBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
//                return new Tuple2<String, Integer>(t1.f0,t1.f1+t2.f1);
//            }
//        }).print();
        //hive flink flink
        //-->flarmap
        //hive
        //flink
        //flink
        //-->map
        //hive 1
        //flink 1
        //flink 1
        //-->groupby
        //hive 1
        //flink (1,1)
        //-->reduce
        //hive 1
        //reduce 2

//如果你reduce的时候不加groupby的话，它默认相加所有的数据，而不是分组，这里和spark不一样
 }
}
