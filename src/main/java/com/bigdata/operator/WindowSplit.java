package com.bigdata.operator;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;


public class WindowSplit {

    public static void main(String[] args) throws Exception {
        long rate = 3;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //DataStream<Tuple2<String, Integer>> grade = WindowJoinSampleData.GradeSource.getSource(env, rate);
        DataStream<Tuple2<String, Integer>> salary = WindowJoinSampleData.SalarySource.getSource(env, rate);
        SplitStream<Tuple2<String, Integer>> split = salary.split(new OutputSelector<Tuple2<String, Integer>>() {
            @Override
            public Iterable<String> select(Tuple2<String, Integer> value) {
                List<String> outPut = new ArrayList<String>();
                if (value.f1 > 5000) {
                    outPut.add("zhong");
                } else {
                    outPut.add("di");
                }
                return outPut;
            }
        });

        split.select("zhong").print();

        env.execute("WindowJoin");

    }

    public static class NameKeySelector implements KeySelector<Tuple2<String,Integer>,String> {

        @Override
        public String getKey(Tuple2<String, Integer> value) throws Exception {
            return value.f0;
        }
    }
}
