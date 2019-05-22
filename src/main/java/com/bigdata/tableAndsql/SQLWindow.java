package com.bigdata.tableAndsql;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;


public class SQLWindow {

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

        stableEnv.registerDataStream("orders",timeStream,"user,product,amomut,rtime.rowtime");

        //Table table = stableEnv.sqlQuery("select * from orders");

        //"YEAR" ...
//        "MONTH" ...
//        "DAY" ...
//        "HOUR" ...
//        "MINUTE" ...
//        "SECOND" ...
        //tumbleing window的写法，和table api还是有很多不同地方的
        Table tumbleTable = stableEnv.sqlQuery("select user,TUMBLE_START(rtime, INTERVAL '1' DAY) as wstart, " +
                "SUM(amomut) from orders " +
                "group by TUMBLE(rtime,INTERVAL '1' DAY),user");

        //slide window写法叫做HOP
        Table hopTable = stableEnv.sqlQuery("select user, " +
                "SUM(amomut) from orders " +
                "group by HOP(rtime,INTERVAL '5' SECOND,INTERVAL '5' SECOND),user");

        //session window的写法
        Table sessionTable = stableEnv.sqlQuery("select user, " +
                "SESSION_START(rtime,INTERVAL '10' SECOND) as sstart," +
                "SESSION_ROWTIME(rtime,INTERVAL '10' SECOND) as stime," +
                "SUM(amomut) from orders " +
                "group by SESSION(rtime,INTERVAL '10' SECOND),user");


        stableEnv.toAppendStream(sessionTable, Row.class).print();

        env.execute("SQLWindow");

    }
}
