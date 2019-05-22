package com.bigdata.Operator

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.util.Random

object DataStreamIteratorScala {

  val bound = 100

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val input: DataStream[(Int, Int)] = env.addSource(new RandomSource)

    def withinBound(value: (Int, Int,Int,Int,Int)) = value._3 < bound && value._4 < bound
    input.map(value =>(value._1,value._2,value._1,value._2,0))
      .iterate((iter:DataStream[(Int,Int,Int,Int,Int)])=>{
        val step = iter.map(value => (value._1,value._2,value._4,value._3+value._4,value._5+1))

        val iterate = step.filter(value=>withinBound(value._1,value._2,value._3,value._4,value._5))

        val out = step.filter(value=>(!withinBound(value._1,value._2,value._3,value._4,value._5)))
        (iterate,out)
      },5000L).print()

    env.execute("DataStreamIteratorScala")
//def withinBound(value: (Int, Int)) = value._1 < Bound && value._2 < Bound
//val numbers: DataStream[((Int, Int), Int)] = input
//  // Map the inputs so that the next Fibonacci numbers can be calculated
//  // while preserving the original input tuple
//  // A counter is attached to the tuple and incremented in every iteration step
//  .map(value => (value._1, value._2, value._1, value._2, 0))
//  .iterate(
//    (iteration: DataStream[(Int, Int, Int, Int, Int)]) => {
//      val step = iteration.map(value =>
//        (value._1, value._2, value._4, value._3 + value._4, value._5 + 1))
//      val feedback = step.filter(value => withinBound(value._3, value._4))
//      val output: DataStream[((Int, Int), Int)] = step
//        .filter(value => !withinBound(value._3, value._4))
//        .map(value => ((value._1, value._2), value._5))
//      (feedback, output)
//    }
//    , 5000L
//  )


  }


  private class RandomSource extends SourceFunction[(Int,Int)]{

    val rnd = new Random()

    var count = 0

    override def cancel(): Unit = ???

    override def run(ctx: SourceContext[(Int, Int)]): Unit = {
      while (count<bound){
        val first = rnd.nextInt(bound / 2 -1)+1
        val second = rnd.nextInt(bound / 2 -1)+1

        ctx.collect((first,second))
        count+=1
        Thread.sleep(50L)
      }
    }
  }

}
