package com.bigdata.tableAndsql

import com.bigdata.Operator.WindowDataSampleScala
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment

object DateSetsqlTime {

  case class Grade (name:String,grade:Int)

  case class Salary (name:String,salary:Int)

  case class Person (name:String,grade:Int,salary:Int)

  def main(args: Array[String]) {

    val rate =3
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tenv = TableEnvironment.getTableEnvironment(env)
    val grade = WindowDataSampleScala.getGradeSource(env,rate)

    //tenv.fromDataStream(grade,'UserActionTimestamp, 'UserActionTime.proctime)

  }

}
