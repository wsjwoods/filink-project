package com.bigdata.tableAndsql

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.types.Row

//import org.apache.flink.streaming.api.scala._
//在将stream装化为table需要定义字段的时候，需要导入这个隐士转换，否则，会出现问题
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.api.common.typeinfo._


object TableProcessScala {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tenv = TableEnvironment.getTableEnvironment(env)

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val order: DataStream[Tuple3[Long, String, Int]] = env.fromCollection(Seq(
    Tuple3(1L, "beer", 3),
    Tuple3(1L, "diaper", 4),
    Tuple3(3L, "rubber", 2),
    Tuple3(2L, "beer", 3),
    Tuple3(2L, "diaper", 4),
    Tuple3(3L, "rubber", 2),
    Tuple3(1L, "beer", 3),
    Tuple3(1L, "diaper", 4),
    Tuple3(3L, "rubber", 2),
    Tuple3(2L, "beer", 3),
    Tuple3(2L, "diaper",2),
    Tuple3(3L, "rubber", 2)
    ))

    //java中直接是一个string的写法，这里不太一样，加上了单引号
    val table = tenv.fromDataStream(order,'user,'product,'amomut,'time.proctime)

    val windowTable = table.window(Tumble over 10.seconds on 'time as 'userprocess)

    val proStream: DataStream[Tuple3[Long, String, Int]] = windowTable.groupBy('userprocess,'user,'product,'amomut)
      .select('user,'product,'amomut)
      .toAppendStream[Tuple3[Long, String, Int]]

    proStream.print()
//    val filter = windowTable.table.filter("amomut>3")
//
//    tenv.toAppendStream(filter).print()

    env.execute("TableProcessScala")

  }

}
