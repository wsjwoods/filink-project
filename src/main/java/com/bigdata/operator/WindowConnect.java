package com.bigdata.operator;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


public class WindowConnect {

    public static void main(String[] args) throws Exception {
        long rate = 3;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> grade = WindowJoinSampleData.GradeSource.getSource(env, rate);
        DataStream<Tuple2<String, Integer>> salary = WindowJoinSampleData.SalarySource.getSource(env, rate);

        ConnectedStreams<Tuple2<String, Integer>, Tuple2<String, Integer>> connect = grade.connect(salary);

        connect.map(new CoMapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>, Object>() {
            @Override
            public Object map1(Tuple2<String, Integer> value) throws Exception {
                return value.f1;
            }

            @Override
            public Object map2(Tuple2<String, Integer> value) throws Exception {
                return value.f1;
            }
        }).print();

        env.execute("WindowJoin");

    }

    public static class NameKeySelector implements KeySelector<Tuple2<String,Integer>,String> {

        @Override
        public String getKey(Tuple2<String, Integer> value) throws Exception {
            return value.f0;
        }
    }
}
