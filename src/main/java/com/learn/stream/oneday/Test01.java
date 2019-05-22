package com.learn.stream.oneday;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test01 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        senv.disableOperatorChaining();

        senv.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(3L, 4L), Tuple2.of(1L, 2L))
                .keyBy(0)
                .map(new RichMapFunction<Tuple2<Long, Long>, Long>() {

                    private IntCounter ic = new IntCounter();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        getRuntimeContext().addAccumulator("ic", ic);
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                    }

                    @Override
                    public Long map(Tuple2<Long, Long> value) throws Exception {
                        this.ic.add(1);
                        return value.f1;
                    }
                })
                .print();

        JobExecutionResult executionResult = senv.execute("Test01");
        Object accumulatorResult = executionResult.getAccumulatorResult("ic");

        System.out.println(accumulatorResult);
    }
}
