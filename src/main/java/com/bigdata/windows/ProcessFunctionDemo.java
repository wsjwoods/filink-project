package com.bigdata.windows;

import com.bigdata.bean.CountWithTimestamp;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
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

        env.socketTextStream("node1", 9999).filter(new FilterFunction<String>() {

            @Override
            public boolean filter(String value) throws Exception {
                return !value.trim().equals("");
            }
        }).map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] split = value.split(" ");
                String word = split[0];
                long timestamp = Long.valueOf(split[1]);
                return new Tuple2<String, Long>(word, timestamp);
            }
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {

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
        }).keyBy(0).process(new ProcessFunction<Tuple2<String,Long>, Tuple2<String, Long>>() {

            /**
             * The implementation of the ProcessFunction that maintains the count and timeouts
             * ProcessFunction的实现，用来维护计数和超时
             */
            //** The state that is maintained by this process function */
            /** process function维持的状态 */
            private ValueState<CountWithTimestamp> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
            }
            @Override
            public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                // retrieve the current count
                // 获取当前的count
                CountWithTimestamp current = state.value();
                if (current == null) {
                    current = new CountWithTimestamp();
                    current.key = value.f0;
                }

                // update the state's count
                // 更新 state 的 count
                current.count++;
                // set the state's timestamp to the record's assigned event time timestamp
                // 将state的时间戳设置为记录的分配事件时间戳

                current.lastModified = ctx.timestamp();
                System.out.println("current.lastModified "+current.lastModified);
                // write the state back
                // 将状态写回
                state.update(current);

                // schedule the next timer 60 seconds from the current event time
                // 从当前事件时间开始计划下一个60秒的定时器
                ctx.timerService().registerEventTimeTimer(current.lastModified + 5000);
            }
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out)
                    throws Exception {

                // get the state for the key that scheduled the timer
                //获取计划定时器的key的状态
                CountWithTimestamp result = state.value();

                System.out.println("timestamp "+ timestamp);

                // 检查是否是过时的定时器或最新的定时器
                System.out.println("result.lastModified "+result.lastModified);
                //if (timestamp > result.lastModified + 5000) {
                    // emit the state on timeout
                    out.collect(new Tuple2<String, Long>(result.key, result.count));
                //}
            }
        }).print();

        env.execute("ProcessFunctionDemo");
    }
}
