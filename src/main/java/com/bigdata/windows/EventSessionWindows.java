package com.bigdata.windows;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;


public class EventSessionWindows {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //默认就是processingTime,这里采用EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<Tuple3<String,Long,Integer>> input = new ArrayList<>();
        //我们设置gab(会话过期时间)3
        input.add(new Tuple3<>("a",1L,1));
        input.add(new Tuple3<>("b",1L,1));
        input.add(new Tuple3<>("b",4L,1));
        input.add(new Tuple3<>("b",5L,1));
        input.add(new Tuple3<>("c",6L,1));

        input.add(new Tuple3<>("a",10L,1));
        input.add(new Tuple3<>("c",15L,1));

        env.addSource(new SourceFunction<Tuple3<String,Long,Integer>>() {
            @Override
            public void run(SourceContext<Tuple3<String, Long, Integer>> ctx) throws Exception {
                for (Tuple3<String, Long, Integer> value:input) {
                    ctx.collectWithTimestamp(value,value.f1);
                    ctx.emitWatermark(new Watermark(value.f1-1));
                }
               //ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
            }

            @Override
            public void cancel() {

            }
        })
                .keyBy(0)
                //withgab就是设置会话过期时间 这个是静态的指定会话过期
                //.window(EventTimeSessionWindows.withGap(Time.milliseconds(3L)))
                //动态指定gab
                .window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple3<String, Long, Integer>>() {
                    @Override
                    public long extract(Tuple3<String, Long, Integer> stringLongIntegerTuple3) {
                        //System.out.println(stringLongIntegerTuple3.f1+1);
                        return stringLongIntegerTuple3.f1+1;
                    }
                }))
                .sum(2)
                .print();

        env.execute("EventSessionWindows");
    }
}
