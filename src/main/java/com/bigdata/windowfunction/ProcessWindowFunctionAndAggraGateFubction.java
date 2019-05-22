package com.bigdata.windowfunction;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;


public class ProcessWindowFunctionAndAggraGateFubction {

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
                .timeWindow(Time.seconds(5))
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return new Tuple2<String, Integer>(value1.f0,value1.f1+value2.f1);
                    }
                }, new ProcessWindowFunction<Tuple2<String,Integer>, Object, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Object> out) throws Exception {
                        Tuple2<String, Integer> count = elements.iterator().next();
                        out.collect(new Tuple2<>(context.window(),count.f0+"-"+count.f1));
                    }
                }).print();
        env.execute("ProcessWindowFunctionAndAggraGateFubction");
    }
}
