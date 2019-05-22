package com.bigdata.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        //启用检查点
        env.enableCheckpointing(1000);
        //使用exactly-once语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointTimeout(10000);

        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        //这个设置是我们程序停止了，checkpoint保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setStateBackend(new FsStateBackend("file:///D:\\workspace\\filink-project\\result\\checkpoint"));
        DataStream<Tuple2<String, Integer>> sumStream = env.socketTextStream("node1", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (String word : value.split(" ")) {
                            out.collect(new Tuple2<String, Integer>(word, 1));
                        }
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        sumStream.print();

        env.execute("first stream wordcount");
    }

    /**
     * // start a checkpoint every 1000 ms
     env.enableCheckpointing(1000)

     // advanced options:

     // set mode to exactly-once (this is the default)
     env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

     // make sure 500 ms of progress happen between checkpoints
     env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)

     // checkpoints have to complete within one minute, or are discarded
     env.getCheckpointConfig.setCheckpointTimeout(60000)

     // prevent the tasks from failing if an error happens in their checkpointing, the checkpoint will just be declined.
     env.getCheckpointConfig.setFailTasksOnCheckpointingErrors(false)

     // allow only one checkpoint to be in progress at the same time
     env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
     */
}
