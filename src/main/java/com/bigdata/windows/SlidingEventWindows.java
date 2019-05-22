package com.bigdata.windows;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;


public class SlidingEventWindows {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //默认就是processingTime,这里采用EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.socketTextStream("node1", 9999).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                if(StringUtils.isNullOrWhitespaceOnly(value)){
                    return false;
                }else{
                    return true;
                }

            }
        }).map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
            @Override
            public Tuple3<String,Long,Integer> map(String value) throws Exception {
                String[] split = value.split(" ");
                String word = split[0];
                long timestamp = Long.valueOf(split[1]);
                return new Tuple3<String, Long, Integer>(word,timestamp,1);
            }
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Long, Integer>>() {

            private long currtnyTimestamp = 0L;
            private long maxOutifOrder = 0L;

            @Override
            public long extractTimestamp(Tuple3<String, Long, Integer> stringLongIntegerTuple3, long l) {

                long timestamp = stringLongIntegerTuple3.f1;
                System.out.println("timestamp "+timestamp+" currtnyTimestamp "+currtnyTimestamp);
                currtnyTimestamp = Math.max(timestamp,currtnyTimestamp);
                return timestamp;

            }

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currtnyTimestamp-maxOutifOrder);
            }
        }).keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
               // .evictor(EventT)
                .sum(2)
                .print();

        env.execute("SlidingEventWindows");

    }
}
