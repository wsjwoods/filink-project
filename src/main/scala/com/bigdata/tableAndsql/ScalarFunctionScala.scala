package com.bigdata.tableAndsql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Types, TableEnvironment}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row

//时刻谨记，要导入这两个隐士转换
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._

object ScalarFunctionScala {

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

    tenv.registerFunction("csvudf",new CsvUDF)

    val table = tenv.sqlQuery("select s1,csvudf(s1) from csvtable group by s1")

    tenv.toRetractStream[Row](table).print()

    env.execute("ScalarFunctionScala")


  }

  class CsvUDF extends ScalarFunction{
    def eval(s:String): Int ={
//      if (s == "iris-setosa") {
//         1
//      }
//      else if (s == "iris-versicolor") {
//         2
//      }
//      else if (s == "iris-virginica") {
//         3
//      }
//      else {
//         0
//      }

      s match {
        case "iris-setosa" =>1
        case "iris-versicolor"=>2
        case "iris-virginica"=>3
        case _=>0
      }
    }
  }

}
