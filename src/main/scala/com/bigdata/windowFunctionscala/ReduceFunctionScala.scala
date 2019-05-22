package com.bigdata.windowFunctionscala

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.StringUtils
import org.apache.flink.streaming.api.scala._

/**
  * Created by jojo on 0016.
  */
object ReduceFunctionScala {

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
        .reduce{(v1,v2)=>{(v1._1,v1._2+v2._2)}}
      .print()

    env.execute("ReduceFunctionScala")
  }

}
