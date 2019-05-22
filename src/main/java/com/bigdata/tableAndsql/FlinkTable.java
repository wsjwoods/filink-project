package com.bigdata.tableAndsql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class FlinkTable {

    public static void main(String[] args) throws Exception {
        //支持流式处理
//        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment stableEnv = TableEnvironment.getTableEnvironment(senv);

        //支持批处理
        ExecutionEnvironment benv = ExecutionEnvironment.getExecutionEnvironment();

        BatchTableEnvironment btableEnv = TableEnvironment.getTableEnvironment(benv);

        DataSet<WordCount> tupleDataSource = benv.fromElements(
                new WordCount("hello", 1),
                (new WordCount("word", 1)),
                (new WordCount("hello", 1)),
                (new WordCount("word", 1)));


        Table table = btableEnv.fromDataSet(tupleDataSource);

        btableEnv.registerTable("helloword",table);

/*        DataSet<WordCount> table1 = btableEnv.toDataSet(table,WordCount.class);

        table1.print();*/

        btableEnv.sqlQuery("select * from helloword");

        benv.execute("FlinkTable");

    }

    public static class WordCount{
        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        private String word;

        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }

        private Integer count;

        public WordCount(String word,Integer count){
            this.word=word;
            this.count=count;
        }

    }

}
