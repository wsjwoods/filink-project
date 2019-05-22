//package com.bigdata.CEP
//
//import org.apache.flink.cep.CEP
//import org.apache.flink.cep.pattern.Pattern
//import org.apache.flink.cep.pattern.conditions.IterativeCondition
//import org.apache.flink.cep.pattern.conditions.IterativeCondition.Context
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.streaming.api.windowing.time.Time
//
//
//object CEPExampleScala {
//
//  def main(args: Array[String]) {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//    val userEvent: DataStream[(Int, Long, String)] = env.fromElements(Tuple3[Int, Long, String](1, 1000L, "login"), Tuple3(2, 3000L, "mai"), Tuple3(1, 2000L, "login"), Tuple3(1, 3000L, "login"), Tuple3(1, 3000L, "fukuan"))
//
//    val pattern: Pattern[Tuple3[Int, Long, String], Tuple3[Int, Long, String]] = Pattern.begin("first")
//      .where(new IterativeCondition[Tuple3[Int, Long, String]] {
//      override def filter(t: Tuple3[Int, Long, String], context: Context[Tuple3[Int, Long, String]]): Boolean = {
//        if(t._3.equals("login")){
//          true
//        }
//        false
//      }
//    }).times(3).within(Time.seconds(5))
//
//
//   // CEP.pattern(userEvent,pattern)
//
//    //patternStream.
//  }
//
//}
