package com.bigdata.tableAndsql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;


public class TableCSVSource {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment benv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment btableEnv = TableEnvironment.getTableEnvironment(benv);

        //字段数组
        String[] field = {"name","age","mail"};
        TypeInformation[] types = {Types.STRING(),Types.INT(),Types.STRING()};

        TableSource csvTableSource = new CsvTableSource("D:\\workspace\\filink-project\\src\\main\\resources\\test.csv", field, types);

        btableEnv.registerTableSource("csvsource",csvTableSource);

        //sql api
        Table table = btableEnv.sqlQuery("select * from csvsource where age <26");

        //table api
        Table proce = table.select("name,age,mail").where("age <26");


        TableSink csvTableSink = new CsvTableSink("D:\\workspace\\filink-project\\src\\main\\resources\\result", "|");

        btableEnv.registerTableSink("csvsinktable",field,types,csvTableSink);

        proce.writeToSink(csvTableSink);

        //table.insertInto("csvsinktable");

        //Table table1 = btableEnv.sqlQuery("select * from csvsinktable");


        benv.execute("TableCSVSource");




    }
}
