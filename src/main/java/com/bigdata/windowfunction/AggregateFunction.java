package com.bigdata.windowfunction;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;


public class AggregateFunction {

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
                //聚合的任务输出每个key  以及每个key后面的累加值  和平均值
                //第一个参数就是输入值    第二个参数是累加器的值   第三个参数就是输出的值
                .aggregate(new org.apache.flink.api.common.functions.AggregateFunction<Tuple2<String,Integer>, Tuple3<String,Long,Long>, Tuple3<String,Long,Double>>() {
                    @Override
                    public Tuple3<String,Long,Long> createAccumulator() {
                        return new Tuple3<String, Long, Long>("",0L,0L);
                    }

                    @Override
                    public Tuple3<String,Long,Long> add(Tuple2<String, Integer> value, Tuple3<String,Long,Long> accumulator) {
                        return new Tuple3<String,Long,Long>(value.f0,accumulator.f1+value.f1,accumulator.f2+1L);
                    }

                    @Override
                    public Tuple3<String,Long,Double> getResult(Tuple3<String,Long,Long> accumulator) {
                        return new Tuple3<String, Long, Double>(accumulator.f0,accumulator.f1,Double.valueOf(accumulator.f1) /accumulator.f2 );
                    }

                    @Override
                    public Tuple3<String,Long,Long> merge(Tuple3<String,Long,Long> a, Tuple3<String,Long,Long> b) {
                        return new Tuple3<String, Long, Long>(a.f0,a.f1+b.f1,a.f2+b.f2);
                    }
                }).print();

        env.execute("AggregateFunction");
    }
}
