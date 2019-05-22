package com.bigdata.tableAndsql;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.OverWindowedTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.table.api.java.Over;


import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;


public class TableOverWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment stableEnv = TableEnvironment.getTableEnvironment(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<Tuple4<Long,String,Integer,Long>> dataList = new ArrayList<>();

        dataList.add(new Tuple4<Long,String,Integer,Long>(1L, "beer", 3,1506405922000L));
        dataList.add(new Tuple4<Long,String,Integer,Long>(1L, "diaper", 4,1506405925000L));
        dataList.add(new Tuple4<Long,String,Integer,Long>(3L, "rubber", 2,1506405932000L));
        dataList.add(new Tuple4<Long,String,Integer,Long>(2L, "beer", 3,1506405937000L));
        dataList.add(new Tuple4<Long,String,Integer,Long>(2L, "diaper", 4,1506405947000L));
        dataList.add(new Tuple4<Long,String,Integer,Long>(3L, "rubber", 2,1506405949000L));
        dataList.add(new Tuple4<Long,String,Integer,Long>(1L, "beer", 3,1506405955000L));
        dataList.add(new Tuple4<Long,String,Integer,Long>(1L, "diaper", 4,1506405959000L));
        dataList.add(new Tuple4<Long,String,Integer,Long>(3L, "rubber", 2,1506405964000L));
        dataList.add(new Tuple4<Long,String,Integer,Long>(2L, "beer", 3,1506405965000L));
        dataList.add(new Tuple4<Long,String,Integer,Long>(2L, "diaper",2, 1506405969000L));
        dataList.add(new Tuple4<Long,String,Integer,Long>(3L, "rubber", 2,1506405974000L));

        DataStreamSource<Tuple4<Long, String, Integer, Long>> t4 = env.fromCollection(dataList);

        //既然是eventtime，那么就要分配watermark
        DataStream<Tuple4<Long, String, Integer, Long>> timeStream = t4.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple4<Long, String, Integer, Long>>() {

            Long current = 0L;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(current);
            }

            @Override
            public long extractTimestamp(Tuple4<Long, String, Integer, Long> element, long previousElementTimestamp) {
                current = element.f3;
                return element.f3;
            }
        });

        Table table = stableEnv.fromDataStream(timeStream, "user,product,amomut,time.rowtime");

        Table window = table.window(
                Over
                .partitionBy("user")
                .orderBy("time")
//                .preceding("UNBOUNDED_ROW")
//                .following("CURRENT_ROW")
                        .preceding("5.rows")
                .as("overwindow")).
        select("user,sum(amomut) over overwindow,product.count over overwindow");

        stableEnv.toAppendStream(window, Row.class).print();

        env.execute("TableOverWindow");
    }
}
