package com.bigdata.tableAndsql;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

/**
 * 实现一个功能：
 * 一列值，乘以另外一列值，再除以个数
 *
 *
 */
public class AggregateFunctionA {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment stableEnv = TableEnvironment.getTableEnvironment(env);

        CsvTableSource build = CsvTableSource
                .builder()
                .path("D:\\workspace\\filink-project\\src\\main\\resources\\iris.csv")
                .field("d1", Types.LONG)
                .field("d2", Types.DOUBLE)
                .field("d3", Types.DOUBLE)
                .field("d4", Types.DOUBLE)
                .field("s1", Types.STRING)
                .fieldDelimiter(",")
                .ignoreParseErrors()
                .build();

        stableEnv.registerTableSource("csvtable",build);

        stableEnv.registerFunction("csvavg",new AggreAvg());

        Table table = stableEnv.sqlQuery("select s1,csvavg(d1,d2) from csvtable group by s1");

        stableEnv.toRetractStream(table, Row.class).print();

        env.execute("AggregateFunctionA");


    }

    public static class AvgAccm{
        private long sum = 0;
        private int count = 0;
    }

    public static class AggreAvg extends AggregateFunction<Long,AvgAccm>{

        @Override
        public AvgAccm createAccumulator() {
            return new AvgAccm();
        }

        @Override
        public Long getValue(AvgAccm accumulator) {
            if(accumulator.count==0){
                return 0L;
            }else {
                return accumulator.sum/accumulator.count;
            }
        }

        public void accumulate(AvgAccm acc,long d1,Double d2){
            acc.sum +=d1*d2;
            acc.count+=d2;
        }
    }
}
