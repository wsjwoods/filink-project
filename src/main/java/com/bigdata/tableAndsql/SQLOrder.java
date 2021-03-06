package com.bigdata.tableAndsql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
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
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;


public class SQLOrder {

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

        //Table sql1 = stableEnv.sqlQuery("select sum(amomut) from " + table + " where product like '%rub%'");

        Table sql1 = stableEnv.sqlQuery("select user,amomut from " + table);
        stableEnv.registerTable("sumorder",sql1);
        //转化为datastream进行输出
        //stableEnv.toAppendStream(sql1, Row.class).print();

        String[] fliedName = {"user","amout"};

        TypeInformation[] filedType = {Types.LONG,Types.INT};

        //CsvTableSink实际上是一个AppendStreamTableSink
        //如果用来存储类似于sum这种聚合的操作的时候，会出现问题
        TableSink csvTableSink = new CsvTableSink("D:\\workspace\\filink-project\\src\\main\\resources\\usersink", "-");

        stableEnv.registerTableSink("csvSink",fliedName,filedType,csvTableSink);

        stableEnv.sqlUpdate("insert into csvSink select * from sumorder");

        env.execute("SQLOrder");
    }
}
