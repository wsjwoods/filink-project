//package com.bigdata.Operator
//
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.util.Collector
//import org.apache.flink.streaming.api.scala._
//
//object WindowCogroupScala {
//    case class Grade (name:String,grade:Int)
//
//    case class Salary (name:String,salary:Int)
//
//    case class Person (name:String,grade:Int,salary:Int)
//  def main(args: Array[String]) {
//    val rate =3
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val grade= WindowDataSampleScala.getGradeSource(env,rate)
//
//    val salary = WindowDataSampleScala.getSalarySource(env,rate)
//    grade.coGroup(salary).where(_.name).equalTo(_.name)
//          .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//      .apply{(t1:Iterator[Grade],t2:Iterator[Salary],out:Collector[Person])=>{
//        for(fir <- t1){
//          for(sec <- t2){
//            out.collect(Person(fir.name,fir.grade,sec.salary))
//          }
//        }
//      }}.print()
////    val connect = grade.connect(salary)
////    connect.map(
////        (gra:Grade)=>gra.grade,
////        (sal:Salary)=>sal.salary
////      ).print()
//
//    env.execute("WindowJoinScala")
//  }
//
//}
