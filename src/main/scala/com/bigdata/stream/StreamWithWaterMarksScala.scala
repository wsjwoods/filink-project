package com.bigdata.stream

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.StringUtils

object StreamWithWaterMarksScala {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text= env.socketTextStream("10.252.2.51", 9999)
      .filter(each=> {
        if (StringUtils.isNullOrWhitespaceOnly(each))
          false
        else
          true
      })
      .map(each=>{
        val tokens = each.split(" ")
        (tokens(0),tokens(1).toLong,1)
      })
          .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.seconds(5)) {
            override def extractTimestamp(t: (String, Long, Int)): Long = {
              t._2
            }
          })
      .keyBy(0)
      .timeWindow(Time.seconds(20))
      .sum(2)
      .print()
      env.execute("water marks")
  }

//  class myself extends AssignerWithPeriodicWatermarks {
//    override def getCurrentWatermark: Watermark = ???
//
//    override def extractTimestamp(t: T, l: Long): Long = ???
//  }
}
