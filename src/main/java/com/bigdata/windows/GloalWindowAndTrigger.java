package com.bigdata.windows;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;


public class GloalWindowAndTrigger {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //默认就是processingTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        env.socketTextStream("node1", 9999).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return !value.trim().equals("");
            }
        }).map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String,Integer> map(String value) throws Exception {
                return new Tuple2<String,Integer>(value,1);
            }
        }).keyBy(0)
                .window(GlobalWindows.create())
                .trigger(CountTrigger.of(5))
                .evictor(TimeEvictor.of(Time.milliseconds(3L)))
                .sum(1)
                .print();

        env.execute("GloalWindowAndTrigger");
    }
}
