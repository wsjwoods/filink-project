package com.bigdata.operator;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class RandomPartition {

    public static void main(String[] args) throws Exception {
        long rate = 3;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> grade = WindowJoinSampleData.GradeSource.getSource(env, rate);

        grade.shuffle();

        env.execute("CustomPartition");
    }
}
