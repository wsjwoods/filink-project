package com.bigdata.stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class CheckPointWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

        env.fromElements(Tuple2.of(1L,3L),Tuple2.of(1L,5L),Tuple2.of(1L,7L),Tuple2.of(1L,4L),Tuple2.of(1L,2L))
                .keyBy(0).print();


        //Thread.sleep(5000);
        env.execute("first value state");
    }
}
