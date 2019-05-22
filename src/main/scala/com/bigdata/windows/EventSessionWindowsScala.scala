package com.bigdata.windows

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{SessionWindowTimeGapExtractor, EventTimeSessionWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

object EventSessionWindowsScala {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val input = List(
      ("a",1L,1),
        ("b",1L,1),
      ("a",3L,1),
        ("b",3L,1),
        ("b",5L,1),
        ("c",6L,1),


        ("c",11L,1)
    )

    env.addSource(new SourceFunction[(String,Long,Int)] {
      override def cancel(): Unit = {}

      override def run(ctx: SourceContext[(String, Long, Int)]): Unit ={
        input.foreach(value=>{
          ctx.collectWithTimestamp(value,value._2)
          ctx.emitWatermark(new Watermark(value._2-1))
        })
      }
    })
      .keyBy(0)
      //.window(EventTimeSessionWindows.withGap(Time.milliseconds(3L)))
        .window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[(String,Long,Int)] {
      override def extract(t: (String,Long,Int)): Long = {
        t._2+1
      }
    }))
      .sum(2)
      .print()

    env.execute("EventSessionWindowsScala")

  }

}
