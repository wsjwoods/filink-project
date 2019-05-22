package com.bigdata.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class DataStreamIterate {

    //这次实现的是将输入一些数，每个数都是(a,b)，先转化成(a,b,a+b)-1,a+b,count)

    private static final int BOUND =100;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Integer, Integer>> inputStram = env.addSource(new RandomSource());
        IterativeStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> iterate = inputStram.map(new MapFunction<Tuple2<Integer, Integer>, Tuple5<Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {

                return new Tuple5<>(value.f0, value.f1, value.f0, value.f1, 0);
            }
        }).iterate(5000);


        SplitStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> split = iterate.map(new MapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple5<Integer, Integer, Integer, Integer, Integer>>() {

            @Override
            public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Tuple5<Integer, Integer, Integer, Integer, Integer> value) throws Exception {
                return new Tuple5<>(value.f0, value.f1, value.f3, value.f2 + value.f3, ++value.f4);
            }
        }).split(new OutputSelector<Tuple5<Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public Iterable<String> select(Tuple5<Integer, Integer, Integer, Integer, Integer> value) {
                List<String> out = new ArrayList<>();
                if (value.f2 < BOUND && value.f3 < BOUND) {
                    out.add("iterate");
                } else {
                    out.add("output");
                }
                return out;
            }
        });
        iterate.closeWith(split.select("iterate"));

        split.select("output").print();

        env.execute("DataStreamIterate");

    }

    private static class RandomSource implements SourceFunction<Tuple2<Integer,Integer>>{

        private Random rnd =new Random();

        private int count = 0;

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
            while (count<BOUND){
                int first = rnd.nextInt(BOUND/2 - 1)+1;
                int secod = rnd.nextInt(BOUND/2 - 1)+1;
                ctx.collect(new Tuple2<>(first,secod));
                count++;
                Thread.sleep(50L);
            }
        }

        @Override
        public void cancel() {

        }
    }
}
