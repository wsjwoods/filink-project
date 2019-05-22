package com.bigdata.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object MapWithStateWord {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromCollection(List(
      (1L,3L),
      (1L,5L),
      (1L,7L),
      (1L,4L),
      (1L,2L)
    )).keyBy(_._1)
      .mapWithState((in:(Long,Long),count:Option[Long])=>{
        count match {
          case Some(c)=>((in._1,c),Some(c+in._2))
          case None=>((in._1,0),Some(in._2))
        }
      })
      .print()

    env.execute("ma with state")
  }

}
