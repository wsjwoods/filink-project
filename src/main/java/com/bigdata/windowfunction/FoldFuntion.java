package com.bigdata.windowfunction;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;


public class FoldFuntion {

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
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Integer>>() {

            private long currtnyTimestamp = 0L;
            private long maxOutifOrder = 1000L;
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currtnyTimestamp-maxOutifOrder);
            }

            //因为是ProcessingTime，所以只要把当前的时间戳赋值给currtnyTimestamp就行了
            //event的话，就是从事件中获取watermark，赋值给currtnyTimestamp
            @Override
            public long extractTimestamp(Tuple2<String, Integer> stringIntegerTuple2, long l) {
                currtnyTimestamp=System.currentTimeMillis();
                return currtnyTimestamp;
            }
        })
                .keyBy(0)
                //
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .fold("", new FoldFunction<Tuple2<String, Integer>, String>() {
                    @Override
                    public String fold(String accumulator, Tuple2<String, Integer> value) throws Exception {
                        return accumulator+"-"+value.f0+"-"+ value.f1;
                    }
                }).print();

        env.execute("FoldFuntion");
    }
}
