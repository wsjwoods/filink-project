package com.bigdata.tableAndsql;

import com.bigdata.operator.WindowJoinSampleData;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class StreamingSql {

    public static void main(String[] args) throws Exception {
        long rate = 3;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment stableEnv = TableEnvironment.getTableEnvironment(env);

        DataStream<Tuple2<String, Integer>> grade = WindowJoinSampleData.GradeSource.getSource(env, rate);

        //Table table = stableEnv.fromDataStream(grade,"name,grade");
        Table table = stableEnv.fromDataStream(grade,"f1,f0");
        //Table select = table.filter("name = 'jerry'");
        table.printSchema();

        DataStream<Row> rowDataStream = stableEnv.toAppendStream(table, Row.class);
        rowDataStream.print();

//        TupleTypeInfo<Tuple2<String, Integer>> tupleType = new TupleTypeInfo<>(Types.STRING(),Types.INT());
//        DataStream<Tuple2<String, Integer>> tuple2DataStream = stableEnv.toAppendStream(select, tupleType);
//
//        tuple2DataStream.print();

        env.execute("StreamingSql");


    }
}
