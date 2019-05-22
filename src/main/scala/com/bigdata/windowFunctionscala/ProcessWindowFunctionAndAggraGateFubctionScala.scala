package com.bigdata.windowFunctionscala

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.{Collector, StringUtils}

/**
  * Created by jojo on 0016.
  */
object ProcessWindowFunctionAndAggraGateFubctionScala {

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
      .timeWindow(Time.seconds(5))
      .reduce(new ReduceFunction[(String,Int)] {
        override def reduce(value1: (String,Int), value2: (String,Int)): (String,Int) = {
          (value1._1,value1._2+value2._2)
        }
      },new ProcessWindowFunction[(String, Int), AnyRef, Tuple, TimeWindow] {
        @throws[Exception]
        override def process(key: Tuple, context: Context, elements: Iterable[(String, Int)], out: Collector[AnyRef]): Unit = {
          val count = elements.iterator.next()
          val key = count._1
          val value = count._2
          out.collect(s"Window ${context.window} key: $key  value $value" )
        }
      }).print()
    env.execute("ProcessWindowFunctionAndAggraGateFubctionScala")
  }
}
