package com.bigdata.stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;


public class StreamWithWaterMarks {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        DataStream<Tuple3<String, Long, Integer>> sum = env.socketTextStream("node1", 9999,"\n")
                .filter(new FilterFunction<String>() {

                    public boolean filter(String value) throws Exception {
                        if (StringUtils.isNullOrWhitespaceOnly(value)) {
                            return false;
                        } else {
                            return true;
                        }

                    }
                })
                .map(new MapFunction<String, Tuple3<String, Long, Integer>>() {

                    public Tuple3<String, Long, Integer> map(String value) throws Exception {
                        String[] split = value.split(" ");
                        return new Tuple3<String, Long, Integer>(split[0], Long.valueOf(split[1]), 1);
                    }
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Long, Integer>>() {

                    private long currenTime = 0L;
                    private final long maxOutOfOrderss = 1000L;

                    @Nullable

                    public Watermark getCurrentWatermark() {
                        return new Watermark(currenTime - maxOutOfOrderss);
                    }


                    public long extractTimestamp(Tuple3<String, Long, Integer> t, long l) {
                        long time = t.f1;
                        System.out.println("time:" + time + " cunrrentTime:" + currenTime);
                        currenTime = Math.max(currenTime, time);
                        return time;
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(20))
                .sum(2);

        sum.print();
        env.execute("first stream wordcount");
    }
}
