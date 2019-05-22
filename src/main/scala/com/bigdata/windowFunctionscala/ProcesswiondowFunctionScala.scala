package com.bigdata.windowFunctionscala

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.{Collector, StringUtils}

/**
  * Created by jojo on 0016.
  */
object ProcesswiondowFunctionScala {

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
      .process(new ProcessWindowFunction[(String, Int), AnyRef, Tuple, TimeWindow] {
        @throws[Exception]
        override def process(key: Tuple, context: Context, elements: Iterable[(String, Int)], out: Collector[AnyRef]): Unit = {
          var count = 0;
          var key = ""
          for(input <- elements){
            key = input._1
            count+=input._2
          }
          out.collect(s"window ${context.window}  key: $key  count: $count ")
        }
      }).print()

    env.execute("ProcesswiondowFunctionScala")
  }

}
