package com.bigdata.windows

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.StringUtils


object ProcessSessionWindowsScala {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.socketTextStream("node1", 9999)
      .filter(each=> {
        if (StringUtils.isNullOrWhitespaceOnly(each))
          false
        else
          true
      })
      .map(t=>(t,1))
      .keyBy(0)
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))
      .sum(1)
      .print()

    env.execute("ProcessSessionWindowsScala")
  }
}
