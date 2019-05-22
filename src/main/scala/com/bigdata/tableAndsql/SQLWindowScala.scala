package com.bigdata.tableAndsql

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.types.Row

//在将stream装化为table需要定义字段的时候，需要导入这个隐士转换，否则，会出现问题
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._

object SQLWindowScala {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tenv = TableEnvironment.getTableEnvironment(env)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val order: DataStream[Tuple4[Long, String, Int,Long]] = env.fromCollection(Seq(
      Tuple4(1L, "beer", 3,1506405922000L),
      Tuple4(1L, "diaper", 4,1506405925000L),
      Tuple4(3L, "rubber", 2,1506405932000L),
      Tuple4(2L, "beer", 3,1506405937000L),
      Tuple4(2L, "diaper", 4,1506405947000L),
      Tuple4(3L, "rubber", 2,1506405949000L),
      Tuple4(1L, "beer", 3,1506405955000L),
      Tuple4(1L, "diaper", 4,1506405959000L),
      Tuple4(3L, "rubber", 2,1506405964000L),
      Tuple4(2L, "beer", 3,1506405965000L),
      Tuple4(2L, "diaper",2, 1506405969000L),
      Tuple4(3L, "rubber", 2,1506405974000L)
    ))

    val orderStream = order.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, String, Int, Long)] {

      var current = 0L

      override def getCurrentWatermark: Watermark = {
        new Watermark(current)
      }

      override def extractTimestamp(element: (Long, String, Int, Long), previousElementTimestamp: Long): Long = {
        current = element._4
        element._4
      }
    })

    tenv.registerDataStream("orders",orderStream,'user,'product,'amomut,'rtime.rowtime)

    val result = tenv.sqlQuery("select * from orders")

    val tumResult = tenv.sqlQuery(
      """
        |select
        |user,
        |TUMBLE_START(rtime, INTERVAL '1' DAY) as wstart,
        |SUM(amomut)
        |from orders
        |group by TUMBLE(rtime,INTERVAL '1' DAY),user
      """.stripMargin
    )

    //HOP WINDOW（和table api的slide对应）
    val hopTable = tenv.sqlQuery(
      "select user,SUM(amomut) from orders group by HOP(rtime,INTERVAL '5' SECOND,INTERVAL '5' SECOND),user"
    )

    //Session window写法(和table api基本一致)
    val sessionTable = tenv.sqlQuery(
      """
        |select
        |user,
        |SESSION_START(rtime,INTERVAL '10' SECOND) as sstart,
        |SESSION_ROWTIME(rtime,INTERVAL '10' SECOND) as stime,
        |SUM(amomut)
        |from orders
        |group by SESSION(rtime,INTERVAL '10' SECOND),user
      """.stripMargin
    )


    tenv.toAppendStream[Row](sessionTable).print()

    env.execute("SQLWindowScala")

  }

}
