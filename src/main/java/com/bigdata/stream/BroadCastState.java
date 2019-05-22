package com.bigdata.stream;

import com.sun.org.apache.bcel.internal.generic.BasicType;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;

import org.apache.flink.util.Collector;

import java.util.Map;


public class BroadCastState {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KeyedStream<Tuple2<Long, Long>, Tuple> tupleStream = env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
                .keyBy(0);

        DataStreamSource<Long> dataource = env.fromElements(3L);

        //因为broadcast state是map state类型
        MapStateDescriptor<String, Long> rule = new MapStateDescriptor<>(
                "Rule",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Long>() {
                })
        );

        //
        BroadcastStream<Long> broadcast = dataource.broadcast(rule);

        tupleStream.connect(broadcast).process(new KeyedBroadcastProcessFunction<Object, Tuple2<Long,Long>, Long, Object>() {

            MapStateDescriptor<String, Long> rule = new MapStateDescriptor<>(
                    "Rule",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(new TypeHint<Long>() {
                    })
            );

            @Override
            public void processBroadcastElement(Long aLong, KeyedBroadcastProcessFunction<Object, Tuple2<Long,Long>, Long, Object>.Context context, Collector<Object> collector) throws Exception {
                context.getBroadcastState(rule).put("rule",aLong);
            }

            @Override
            public void processElement(Tuple2<Long, Long> longLongTuple2, KeyedBroadcastProcessFunction<Object, Tuple2<Long,Long>, Long, Object>.ReadOnlyContext readOnlyContext, Collector<Object> collector) throws Exception {
                for (Map.Entry<String,Long> entry:
                readOnlyContext.getBroadcastState(rule).immutableEntries()) {

                    String ruleName = entry.getKey();
                    Long ruleValue = entry.getValue();

                    if(longLongTuple2.f1>ruleValue){
                        collector.collect(longLongTuple2.f0+"\t"+longLongTuple2.f1);
                    }
                }
                ;
            }
        }).print();



    }
}
