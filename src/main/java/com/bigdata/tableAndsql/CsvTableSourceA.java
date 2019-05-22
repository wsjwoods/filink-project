package com.bigdata.tableAndsql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.api.common.typeinfo.Types;


public class CsvTableSourceA {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment stableEnv = TableEnvironment.getTableEnvironment(env);

        CsvTableSource build = CsvTableSource
                .builder()
                .path("D:\\workspace\\filink-project\\src\\main\\resources\\iris.csv")
                .field("d1", Types.DOUBLE)
                .field("d2", Types.DOUBLE)
                .field("d3", Types.DOUBLE)
                .field("d4", Types.DOUBLE)
                .field("s1", Types.STRING)
                .fieldDelimiter(",")
                .ignoreParseErrors()
                .build();

        stableEnv.registerTableSource("csvtable",build);

        Table table = stableEnv.sqlQuery("select * from csvtable where s1='setosa'");

//        path   The output path to write the Table to.
//        fieldDelim    The field delimiter.
//        numFiles    The number of files to write to.
//        writeMode   The write mode to specify whether existing files are overwritten or not.
        table.writeToSink(new CsvTableSink(
                "D:\\workspace\\filink-project\\src\\main\\resources\\csvresu",
                " | ",
                1,
                org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE));

        env.execute("CsvTableSourceA");
    }
}
