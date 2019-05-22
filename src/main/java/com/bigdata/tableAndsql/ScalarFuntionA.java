package com.bigdata.tableAndsql;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;


public class ScalarFuntionA {

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

        stableEnv.registerFunction("csvudf",new CsvUDF());

        Table table = stableEnv.sqlQuery("select s1,csvudf(s1) from csvtable group by s1");

        stableEnv.toRetractStream(table, Row.class).print();

        env.execute("ScalarFuntionA");

    }

    public static class CsvUDF extends ScalarFunction{
        public int eval(String s){
            if(s.equals("iris-setosa")){
                return 1;
            }else if(s.equals("iris-versicolor")){
                return 2;
            }else if(s.equals("iris-virginica")){
                return 3;
            }else {
                return 0;
            }
        }
    }
}
