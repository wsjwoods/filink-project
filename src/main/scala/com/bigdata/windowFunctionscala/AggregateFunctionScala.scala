package com.bigdata.windowFunctionscala

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.StringUtils

object AggregateFunctionScala {

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
      .aggregate(new AggregateFunction[(String,Int),(String,Long,Long),(String,Long,Double)] {

        override def createAccumulator(): (String, Long, Long) = ("",0L,0L)

        override def merge(a: (String, Long, Long), b: (String, Long, Long)): (String, Long, Long) = {
          (a._1,a._2+b._2,a._3+b._3)
        }

        override def getResult(accumulator: (String, Long, Long)): (String, Long, Double) = {
          (accumulator._1,accumulator._2,accumulator._2/accumulator._3)
        }

        override def add(value: (String, Int), accumulator: (String, Long, Long)): (String, Long, Long) = {
          (value._1,accumulator._2+value._2,accumulator._3+1L)
        }
      }).print()

    env.execute("AggregateFunctionScala")
  }

}
