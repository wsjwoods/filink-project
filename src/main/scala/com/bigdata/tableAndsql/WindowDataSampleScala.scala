//package com.bigdata.tableAndsql
//
////import com.bigdata.Operator.WindowCogroupScala.{Salary, Grade}
//import com.bigdata.Operator.WindowSplitScala.{Grade, Salary}
//import com.bigdata.operator.ThrottIterator
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
//
//import scala.collection.JavaConverters._
//import scala.util.Random
//

//object WindowDataSampleScala {
//
//    val NAMES = Array("tom", "jerry", "alice", "bob", "john", "garce")
//    val GRADE_COUNT = 5
//    val SALARY_MAX = 10000
//
//    def getGradeSource(env:StreamExecutionEnvironment,rate:Long):DataStream[Grade]={
//      env.fromCollection(new ThrottIterator(new GradeSource().asJava,rate).asScala)
//    }
//
//    def getSalarySource(env:StreamExecutionEnvironment,rate:Long):DataStream[Salary]={
//      env.fromCollection(new ThrottIterator(new SalarySource().asJava,rate).asScala)
//    }
//
//    class GradeSource extends Iterator[Grade] with Serializable{
//      val rnd = new Random(hashCode())
//
//      override def hasNext: Boolean = true
//
//      override def next(): Grade = Grade(NAMES(rnd.nextInt(NAMES.length)),rnd.nextInt(GRADE_COUNT)+1)
//    }
//
//    class SalarySource extends Iterator[Salary] with Serializable{
//      val rnd = new Random(hashCode())
//      override def hasNext: Boolean = true
//
//      override def next(): Salary = Salary(NAMES(rnd.nextInt(NAMES.length)),rnd.nextInt(SALARY_MAX)+1)
//    }
//
//}
