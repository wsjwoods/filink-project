package com.bigdata.windowfunction;

import com.bigdata.bean.CountWithTimestamp;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;


public class ProcessFunctionDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //默认就是processingTime,这里采用EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

        env.socketTextStream("node1", 9999).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return !value.trim().equals("");
            }
        }).map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String,Long> map(String value) throws Exception {
                String[] split = value.split(" ");
                String word = split[0];
                long timestamp = Long.valueOf(split[1]);
                return new Tuple2<String, Long>(word,timestamp);
            }
        })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {

                    private long currtnyTimestamp = 0L;
                    private long maxOutifOrder = 0L;

                    @Override
                    public long extractTimestamp(Tuple2<String, Long> stringLongIntegerTuple3, long l) {

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
                })
                .keyBy(0)
                .process(new ProcessFunction<Tuple2<String,Long>, Tuple2<String,Long>>() {
                    //用ProcessFunction维持一个计数，并且，5秒输出一次
                    //我们想5秒输出一次，但是，并没有window，怎么办，可以用定时器
                    private ValueState<CountWithTimestamp>  state;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        state = getRuntimeContext().getState(new ValueStateDescriptor<>("mystate",CountWithTimestamp.class));
                    }

                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                        CountWithTimestamp currentValue = state.value();
                        if(currentValue == null){
                            currentValue = new CountWithTimestamp();
                            currentValue.key = value.f0;
                        }
                        currentValue.count++;

                        currentValue.lastModified = ctx.timestamp();
                        System.out.println("currentValue.lastModified "+currentValue.lastModified);

                        state.update(currentValue);

                        ctx.timerService().registerEventTimeTimer(currentValue.lastModified+5000);
                    }
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                        CountWithTimestamp value = state.value();
                        System.out.println("timestamp: "+timestamp);
                        System.out.println("lastModified "+value.lastModified);
                        out.collect(new Tuple2<>(value.key,value.count));
                    }
                }).print();

        env.execute("ProcessFunctionDemo");
    }
}
