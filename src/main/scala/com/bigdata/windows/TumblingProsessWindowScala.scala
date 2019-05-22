package com.bigdata.windows

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.StringUtils
import org.apache.flink.streaming.api.scala._

object TumblingProsessWindowScala {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val text= env.socketTextStream("node1", 9999)
      .filter(each=> {
        if (StringUtils.isNullOrWhitespaceOnly(each))
          false
        else
          true
      })
      .map(t=>(t,1))
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String,Int)] {
        val late =1000L
        var curr = 0L
        override def getCurrentWatermark: Watermark = new Watermark(curr-late)

        override def extractTimestamp(t: (String, Int), l: Long): Long = {
          curr = System.currentTimeMillis()
          curr
        }
      })
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .sum(1)
        .print()

    env.execute("TumblingProsessWindowScala")
  }

}
