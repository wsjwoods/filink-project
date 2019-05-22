package com.bigdata.tableAndsql;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;


public class TableFunctionA {

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

        stableEnv.registerFunction("split",new split("-"));

        Table table = stableEnv.sqlQuery("select s1,father,child from csvtable,LATERAL TABLE(split(s1)) as T(father,child)");

        stableEnv.toAppendStream(table, Row.class).print();

        env.execute("TableFunctionA");

    }

    public static class split extends TableFunction<Tuple2<String,String>>{
        private String separator = " ";

        public split(String separator) {
            this.separator = separator;
        }

        public void eval(String str){
            String[] split = str.split(separator);
            collect(new Tuple2<>(split[0],split[1]));
        }
    }
}
