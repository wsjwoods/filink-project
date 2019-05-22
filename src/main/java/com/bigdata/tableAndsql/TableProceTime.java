package com.bigdata.tableAndsql;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.WindowedTable;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;


public class TableProceTime {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment stableEnv = TableEnvironment.getTableEnvironment(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        List<Tuple3<Long,String,Integer>> dataList = new ArrayList<>();
        dataList.add(new Tuple3<Long,String,Integer>(1L, "beer", 3));
        dataList.add(new Tuple3<Long,String,Integer>(1L, "diaper", 4));
        dataList.add(new Tuple3<Long,String,Integer>(3L, "rubber", 2));
        dataList.add(new Tuple3<Long,String,Integer>(2L, "beer", 3));
        dataList.add(new Tuple3<Long,String,Integer>(2L, "diaper", 4));
        dataList.add(new Tuple3<Long,String,Integer>(3L, "rubber", 2));
        dataList.add(new Tuple3<Long,String,Integer>(1L, "beer", 3));
        dataList.add(new Tuple3<Long,String,Integer>(1L, "diaper", 4));
        dataList.add(new Tuple3<Long,String,Integer>(3L, "rubber", 2));
        dataList.add(new Tuple3<Long,String,Integer>(2L, "beer", 3));
        dataList.add(new Tuple3<Long,String,Integer>(2L, "diaper",2));
        dataList.add(new Tuple3<Long,String,Integer>(3L, "rubber", 2));

        DataStreamSource<Tuple3<Long, String, Integer>> listStream = env.fromCollection(dataList);

        Table table = stableEnv.fromDataStream(listStream, "user,product,amomut,time.proctime");

        //为什么要起别名
        //因为，一个流中，可能windowtable有多个，起别名是为了唯一性
        WindowedTable window = table.window(Tumble.over("10.minutes").on("time").as("userprocess"));

        Table filter = window.table().filter("amomut>3");

        stableEnv.toAppendStream(filter, Row.class).print();

        env.execute("TableProceTime");
    }
}
