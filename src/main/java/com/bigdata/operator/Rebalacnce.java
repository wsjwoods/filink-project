package com.bigdata.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Rebalacnce {

    public static void main(String[] args) throws Exception {
        long rate = 3;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> grade = WindowJoinSampleData.GradeSource.getSource(env, rate);

        grade.map(new MapFunction<Tuple2<String,Integer>, Object>() {
            @Override
            public Object map(Tuple2<String, Integer> value) throws Exception {
                return null;
            }
        }).slotSharingGroup("name");
        //grade.rebalance();
        grade.map(new MapFunction<Tuple2<String,Integer>, Object>() {
            @Override
            public Object map(Tuple2<String, Integer> value) throws Exception {
                return null;
            }
        }).startNewChain().map(new MapFunction<Object, Object>() {

            @Override
            public Object map(Object value) throws Exception {
                return null;
            }
        });

        env.execute("CustomPartition");
    }
}
