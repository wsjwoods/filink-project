package com.bigdata.windows

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.StringUtils
import org.apache.flink.streaming.api.scala._


object TumblingEventWindowScala {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val text= env.socketTextStream("node1", 9999)
      .filter(each=> {
        if (StringUtils.isNullOrWhitespaceOnly(each))
          false
        else
          true
      })
      .map(t=>{
        val spil = t.split(" ")
        val word = spil(0)
        val timestap = spil(1).toLong
        (word,timestap,1)
      })
         .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long, Int)] {
           val late =1000L
           var curr = 0L
           override def getCurrentWatermark: Watermark = new Watermark(curr-late)

           override def extractTimestamp(t: (String, Long, Int), l: Long): Long = {
             val timestamp = t._2
             println("timestamp "+timestamp+" curr "+curr)
             curr = Math.max(curr,timestamp)
             timestamp
           }
         })
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .sum(2)
        .print()
    env.execute("TumblingEventWindowScala")
  }

}
