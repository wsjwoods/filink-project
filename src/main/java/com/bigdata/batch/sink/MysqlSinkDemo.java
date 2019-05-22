package com.bigdata.batch.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

public class MysqlSinkDemo {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<String, Integer, String>> types = env.readCsvFile("D:\\workspace\\filink-project\\src\\main\\resources\\test.csv")
                .types(String.class, Integer.class, String.class);


        DataSet<Row> aggregate = types.groupBy(0).aggregate(Aggregations.SUM, 1)
                .map(new MapFunction<Tuple3<String, Integer, String>, Row>() {
                    @Override
                    public Row map(Tuple3<String, Integer, String> t) throws Exception {
                        return Row.of(t.f0,t.f1.toString(),t.f2);
                    }
                });

        aggregate.output(
                JDBCOutputFormat.buildJDBCOutputFormat()
                        .setDrivername("com.mysql.jdbc.Driver")
                        .setDBUrl("jdbc:mysql://192.168.148.12:3306/test")
                        //   .setFetchSize(2)
                        .setUsername("root")
                        .setPassword("123456")
                        //如果，你的row的顺序和mysql表的字段是一一对应的，那么，可以不写(name，age，phone)
                        .setQuery("insert into People values(?,?,?)")
        .finish());


        env.execute();

    }
}
