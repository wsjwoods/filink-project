package com.bigdata.batch.sink;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

public class TextFileSinkDemo {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> wordCountSource = env.readTextFile("D:\\workspace\\filink-project\\src\\main\\resources\\wordcount");

        DataSet<Tuple2<String, Integer>> mapPartitionOperator = wordCountSource.mapPartition(new MapPartitionFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void mapPartition(Iterable<String> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String line : iterable) {
                    String[] split = line.split(" ");
                    for (String word : split) {
                        collector.collect(new Tuple2<>(word, 1));
                    }
                }
            }
        });

        DataSet<Tuple2<String, Integer>> aggregate = mapPartitionOperator.groupBy(0).aggregate(Aggregations.SUM, 1);

        //aggregate.print();
        //1.为什么是8个文件？
        //      因为，我们在执行groupby的时候，有8个group，也就是8个分区，所以，会产生8个文件
        //2.为什么有的文件有内容，为什么有的没有？
        //
        //3.文件存在的话会报错，这应该怎么办？
         //     默认不会覆盖，这个时候，我们需要WriteMode.OVERWRITE参数，存在的话，就进行覆盖
        //      默认有两个参数，NO_OVERWRITE,OVERWRITE;

        //4.如果，我只想输出一个文件，应该怎么办？(避免小文件过多的问题)
        //      //partitionByHash()   partitionByRange  partitionCustom
        //      只想输出一个文件，本质上，就是修改它的分区数就可以
        //      setParallelism这个过低，也有问题，就是写出的速度会很慢


        aggregate.writeAsText("D:\\workspace\\filink-project\\result\\wordcount", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

    }
}
