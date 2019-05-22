package com.learn.stream.oneday;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class Test02 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        senv.setBufferTimeout(10);

        DataStreamSource<Tuple2<String, Integer>> streamSource = senv.addSource(new SourceFunction<Tuple2<String, Integer>>() {
            private long num = 0;
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
                while (isRunning) {
                    String key = "t-" + num;
                    ctx.collect(new Tuple2<>(key, new Random().nextInt(100)));
                    num++;
                    Thread.sleep(2000);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = streamSource.keyBy(0);
        keyedStream.print();


        JobExecutionResult executionResult = senv.execute("Test02");
    }
}
