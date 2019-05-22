package com.bigdata.stream

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._


object StateWordCountScala {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromCollection(List(
      (1L,3L),
      (1L,5L),
      (1L,7L),
      (1L,4L),
      (1L,2L)
    )).keyBy(_._1)
      .flatMap(new wordCountState).print()

    env.execute("state wordcount")
  }

  class wordCountState extends RichFlatMapFunction[(Long,Long),(Long,Long)] {

    var sum : ValueState[(Long,Long)] = _
    override def flatMap(value: (Long, Long), out: Collector[(Long, Long)]): Unit = {

      val newSum = (sum.value()._1+1,sum.value()._2+value._2)

      sum.update(newSum)

      if(newSum._1 >=2){
        out.collect((value._1,newSum._2/newSum._1))
        sum.clear()
      }
    }


    override def open(parameters: Configuration): Unit = {
      sum = getRuntimeContext.getState(new ValueStateDescriptor[(Long,Long)](
        "averder",
        TypeInformation.of(classOf[(Long,Long)]),
        (0L,0L)
      ))
    }
  }

}
