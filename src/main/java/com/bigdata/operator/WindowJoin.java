package com.bigdata.operator;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


public class WindowJoin {

    public static void main(String[] args) throws Exception {
        long rate = 3;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> grade = WindowJoinSampleData.GradeSource.getSource(env, rate);
        DataStream<Tuple2<String, Integer>> salary = WindowJoinSampleData.SalarySource.getSource(env, rate);

        grade.join(salary).where(new NameKeySelector()).equalTo(new NameKeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Tuple2<String,Integer>, Tuple2<String,Integer>, Object>() {
                    @Override
                    public Tuple3<String,Integer,Integer> join(Tuple2<String, Integer> first, Tuple2<String, Integer> second) throws Exception {
                        return new Tuple3<String, Integer, Integer>(first.f0,first.f1,second.f1);
                    }
                }).print();
        //grade.project(1,0).print();

        env.execute("WindowJoin");

    }

    public static class NameKeySelector implements KeySelector<Tuple2<String,Integer>,String>{

        @Override
        public String getKey(Tuple2<String, Integer> value) throws Exception {
            return value.f0;
        }
    }
}
