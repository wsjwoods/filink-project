package com.bigdata.CEP;

import com.bigdata.CEP.event.MonitorEvent;
import com.bigdata.CEP.event.TemperatureAlert;
import com.bigdata.CEP.event.TemperatureEvent;
import com.bigdata.CEP.event.TemperatureWaring;
import com.bigdata.CEP.source.MonitoringEventSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * 场景：
 * 有一些服务器，服务器有温度，如果服务器的温度超过一定阈值的时候就应该告警，所以，本次需求是对
 * 服务器的温度进行监控和报警
 * 需求：
 * 现在服务器有一个阈值  threld
 * 如果服务温度2次超过了这个阈值，我们就发出warn
 * 如果服务器在20秒钟之内发出了两次warn，并且，第二次告警温度比第一次warn的温度高，就alert
 */
public class CEPMonitoring {
    //温度的阈值
    private static final double TEMPERATURE_THRESOLD=100;
    //生成机架id使用
    private static final int MaxRack_ID =10;
    //多久生成一次  100ms
    private static final long PAUSE = 100;
    //生成温度的概率
    private static final double TEMPERATURE_RATIO = 0.5;
    //根据高斯分布来生成温度
    private static final double TEMPERATURE_STD=20;
    private static final double TEMPERATURE_MEAN=80;
    private static final double POWER_STD=10;
    private static final double POWER_MEAN=100;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<MonitorEvent> monitorEvent = env.addSource(
                new MonitoringEventSource(
                        MaxRack_ID,
                        PAUSE,
                        TEMPERATURE_RATIO,
                        TEMPERATURE_STD,
                        TEMPERATURE_MEAN,
                        POWER_STD,
                        POWER_MEAN
                )
        ).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<MonitorEvent>() {

            private long current;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                long now = Math.max(System.currentTimeMillis(), current);
                current = now;
                return new Watermark(now - 1);
            }

            @Override
            public long extractTimestamp(MonitorEvent element, long previousElementTimestamp) {

                long now = Math.max(System.currentTimeMillis(), current);
                current = now;
                return now;
            }
        }).keyBy("rackID");

        Pattern<MonitorEvent, TemperatureEvent> pattern = Pattern.<MonitorEvent>begin("first")
                .subtype(TemperatureEvent.class)
                .where(new IterativeCondition<TemperatureEvent>() {
                    @Override
                    public boolean filter(TemperatureEvent temperatureEvent, Context<TemperatureEvent> context) throws Exception {
                        return temperatureEvent.getTemperature() >= TEMPERATURE_THRESOLD;
                    }
                })
                .next("second")
                .subtype(TemperatureEvent.class)
                .where(new IterativeCondition<TemperatureEvent>() {
                    @Override
                    public boolean filter(TemperatureEvent temperatureEvent, Context<TemperatureEvent> context) throws Exception {
                        return temperatureEvent.getTemperature() >= TEMPERATURE_THRESOLD;
                    }
                }).within(Time.seconds(10));


        PatternStream<MonitorEvent> pattern1 = CEP.pattern(monitorEvent, pattern);

        DataStream<Tuple2<Integer,Double>> warn = pattern1.select(new PatternSelectFunction<MonitorEvent, Tuple2<Integer,Double>>() {
            @Override
            public Tuple2<Integer,Double> select(Map<String, List<MonitorEvent>> map) throws Exception {
                TemperatureEvent first = (TemperatureEvent) map.get("first").get(0);
                TemperatureEvent second = (TemperatureEvent) map.get("second").get(0);
                return new Tuple2<Integer,Double>(first.getRackID(), (first.getTemperature() + second.getTemperature()) / 2);
            }
        }).keyBy(0);

        Pattern<Tuple2<Integer,Double>, Tuple2<Integer,Double>> alertPattern = Pattern.<Tuple2<Integer,Double>>begin("first")
                .next("second")
                .within(Time.seconds(20));

        PatternStream<Tuple2<Integer,Double>> alertPatternStream = CEP.pattern(warn, alertPattern);

        DataStream<TemperatureAlert> alertStrem = alertPatternStream.flatSelect(new PatternFlatSelectFunction<Tuple2<Integer,Double>, TemperatureAlert>() {
            @Override
            public void flatSelect(Map<String, List<Tuple2<Integer,Double>>> map, Collector<TemperatureAlert> collector) throws Exception {
                Tuple2<Integer,Double> first = map.get("first").get(0);
                Tuple2<Integer,Double> second = map.get("second").get(0);
                if (first.f1 < second.f1) {
                    collector.collect(new TemperatureAlert(first.f0));
                }
            }
        });

        warn.print();
        alertStrem.print();

        env.execute("CEPMonitoring");
    }
}
