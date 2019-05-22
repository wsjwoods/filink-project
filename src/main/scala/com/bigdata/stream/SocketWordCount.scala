package com.bigdata.stream

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object SocketWordCount {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //启用检查点
    env.enableCheckpointing(1000)
    //使用exactly-once语义
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    env.getCheckpointConfig.setCheckpointTimeout(10000)

    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)

    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    //这个设置是我们程序停止了，checkpoint保留
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    env.setStateBackend(new FsStateBackend("file:///D:\\workspace\\filink-project\\result\\checkpoint"))
    val text: DataStream[String] = env.socketTextStream("node1", 9999)

    text.flatMap(_.split(" "))
      .map((_,1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
      .print()

    env.execute("first word count")
  }

}
