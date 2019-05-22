package com.bigdata.stream;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;


public class OperatuonStateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(Tuple2.of(1L,3L),Tuple2.of(1L,5L),Tuple2.of(1L,7L),Tuple2.of(1L,4L),Tuple2.of(1L,2L))
                .keyBy(0)
                //.flatMap(new wordCountState())
                .addSink(new BufferingSink());


        env.execute("operate state");
    }

    public static class wordCountState extends RichFlatMapFunction<Tuple2<Long,Long>,Tuple2<Long,Long>> {

        ValueState<Tuple2<Long, Long>> sum;
        @Override
        public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
            //获取state的value
            Tuple2<Long, Long> currentValue = sum.value();
            //跟新count
            currentValue.f0+=1;

            currentValue.f1+=value.f1;

            sum.update(currentValue);
            if(currentValue.f0>=2){
                out.collect(new Tuple2<Long, Long>(value.f0,currentValue.f1/currentValue.f0));
                sum.clear();
            }
        }
        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Tuple2<Long,Long>> descriptor =
                    new ValueStateDescriptor<Tuple2<Long, Long>>(
                            "averade",
                            TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                            }),
                            Tuple2.of(0L,0L)
                    );
            sum = getRuntimeContext().getState(descriptor);
        }
    }

    public static class BufferingSink implements SinkFunction<Tuple2<Long,Long>>,
            CheckpointedFunction{

        ListState<Tuple2<Long, Long>> checkState;

        List<Tuple2<Long,Long>> buffedElement = new ArrayList<>();

        int threshold=2;

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            checkState.clear();
            for (Tuple2<Long,Long> element:buffedElement) {
                checkState.add(element);
            }
            //checkState.addAll(buffedElement);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Tuple2<Long, Long>> descriptor = new ListStateDescriptor<>(
                    "buffering-elements",
                    TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                    })
            );
            checkState =context.getOperatorStateStore().getListState(descriptor);
            if(context.isRestored()){
                for (Tuple2<Long,Long> element:checkState.get()) {
                    buffedElement.add(element);
                }
            }
        }

        @Override
        public void invoke(Tuple2<Long, Long> value, Context context) throws Exception {
            buffedElement.add(value);
            if(buffedElement.size()==threshold){
                for (Tuple2<Long,Long> element:buffedElement) {
                    System.out.println(element.f0+"\t"+element.f1);
                }
                buffedElement.clear();
            }
        }
    }
}
