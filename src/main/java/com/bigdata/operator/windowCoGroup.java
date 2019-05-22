package com.bigdata.operator;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


public class windowCoGroup {

    public static void main(String[] args) throws Exception {
        long rate = 3;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> grade = WindowJoinSampleData.GradeSource.getSource(env, rate);
        DataStream<Tuple2<String, Integer>> salary = WindowJoinSampleData.SalarySource.getSource(env, rate);

        grade.coGroup(salary).where(new NameKeySelector()).equalTo(new NameKeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Tuple2<String,Integer>, Tuple2<String,Integer>, Object>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<String, Integer>> first, Iterable<Tuple2<String, Integer>> second, Collector<Object> out) throws Exception {

                        for (Tuple2<String, Integer> fir:first) {
                            for (Tuple2<String, Integer> sec:second) {
                                System.out.println(fir.f1+"-"+sec.f1);
                                out.collect(new Tuple3<>(fir.f0,fir.f1,sec.f1));
                            }
                        }
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
