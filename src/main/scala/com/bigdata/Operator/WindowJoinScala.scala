package com.bigdata.Operator

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time


object WindowJoinScala {

  case class Grade (name:String,grade:Int)

  case class Salary (name:String,salary:Int)

  case class Person (name:String,grade:Int,salary:Int)

  def main(args: Array[String]) {
    val rate =3
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val grade = WindowDataSampleScala.getGradeSource(env,rate)

    val salary = WindowDataSampleScala.getSalarySource(env,rate)
    grade.join(salary).where(_.name).equalTo(_.name)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
        .apply{(g,s)=>Person(g.name,g.grade,s.salary)}
      .print()

    env.execute("WindowJoinScala")


  }
}
