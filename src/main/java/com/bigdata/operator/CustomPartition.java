package com.bigdata.operator;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class CustomPartition {

    public static void main(String[] args) throws Exception {
        long rate = 3;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> grade = WindowJoinSampleData.GradeSource.getSource(env, rate);

        grade.partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer key, int numPartitions) {
                int partion = key.hashCode()%numPartitions;
                System.out.println(numPartitions);
                System.out.println("partion: "+partion+" grade: "+key);
                return partion;
            }
        },1).print();

        env.execute("CustomPartition");
    }

}
