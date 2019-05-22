package com.bigdata.windows;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.StringUtils;


public class ProcessSessionWindows {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //默认就是processingTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        env.socketTextStream("node1", 9999).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                if(StringUtils.isNullOrWhitespaceOnly(value)){
                    return false;
                }
                return true;
            }
        }).map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String,Integer> map(String value) throws Exception {
                return new Tuple2<String,Integer>(value,1);
            }
        }).keyBy(0)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))
//                .window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple2<String, Integer>>() {
//                    @Override
//                    public long extract(Tuple2<String, Integer> stringIntegerTuple2) {
//                        return 0;
//                    }
//                })))
                .sum(1)
                .print();

        env.execute("ProcessSessionWindows");
    }
}
