package com.bigdata.tableAndsql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Types, TableEnvironment}
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row
//时刻谨记，要导入这两个隐士转换
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._

object AggregateFunctionScala {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tenv = TableEnvironment.getTableEnvironment(env)

    val csvtable = CsvTableSource.builder()
      .path("D:\\workspace\\filink-project\\src\\main\\resources\\iris.csv")
      .field("d1", Types.LONG)
      .field("d2", Types.DOUBLE)
      .field("d3", Types.DOUBLE)
      .field("d4", Types.DOUBLE)
      .field("s1", Types.STRING)
      .fieldDelimiter(",")
      .ignoreParseErrors()
      .build()

    tenv.registerTableSource("csvtable",csvtable)

    tenv.registerFunction("csvaggre",new WeAvg)

    val table = tenv.sqlQuery("select s1,csvaggre(d1,d2) from csvtable group by s1")

    tenv.toRetractStream[Row](table).print()

    env.execute("AggregateFunctionScala")
  }

  class AggAvg{
    var sum = 0
    var count = 0.0
  }

  class WeAvg extends AggregateFunction[Long,AggAvg]{
    override def createAccumulator(): AggAvg = {
      new AggAvg
    }

    override def getValue(accumulator: AggAvg): Long = {
      if(accumulator.count == 0){
        0L
      }else{
        (accumulator.sum/accumulator.count).toLong
      }
    }

    def accumulate(aa:AggAvg,d1:Long,d2:Double): Unit ={
      aa.sum +=(d1*d2).toInt
      aa.count=d2
    }
  }

}
