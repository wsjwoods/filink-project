package com.bigdata.tableAndsql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Types, TableEnvironment}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row

//时刻谨记，要导入这两个隐士转换
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._

object TableFunctionScala {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tenv = TableEnvironment.getTableEnvironment(env)

    val csvtable = CsvTableSource.builder()
      .path("D:\\workspace\\filink-project\\src\\main\\resources\\iris.csv")
      .field("d1", Types.DOUBLE)
      .field("d2", Types.DOUBLE)
      .field("d3", Types.DOUBLE)
      .field("d4", Types.DOUBLE)
      .field("s1", Types.STRING)
      .fieldDelimiter(",")
      .ignoreParseErrors()
      .build()

    tenv.registerTableSource("csvtable",csvtable)
    tenv.registerFunction("split",new split)

    val table = tenv.sqlQuery("select s1,father,child from csvtable,LATERAL TABLE(split(s1)) as T(father,child)")

    tenv.toAppendStream[Row](table).print()

    env.execute("TableFunctionScala")
  }

  class split extends TableFunction[Tuple2[String,String]]{
    //val separator:String
    def eval(str:String): Unit ={
        val splitw = str.split("-")
      collect(new Tuple2[String,String](splitw(0),splitw(1)))
    }
  }

}
