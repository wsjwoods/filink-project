package com.bigdata.Operator

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object WindowSplitScala {

  case class Grade (name:String,grade:Int)

  case class Salary (name:String,salary:Int)

  case class Person (name:String,grade:Int,salary:Int)

  def main(args: Array[String]) {
    val rate =3
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //val grade= WindowDataSampleScala.getGradeSource(env,rate)

    val salary = WindowDataSampleScala.getSalarySource(env,rate)
    val splitstram = salary.split(
      (sal:Salary)=>
        (sal.salary > 5000) match {
          case true=>List("zhong")
          case false=>List("di")
        }
    )

    splitstram.select("zhong").print()
    env.execute("WindowSplitScala")

  }

}
