package com.bigdata.tableAndsql;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;


public class TableOperatorBach {

    public static void main(String[] args) throws Exception {
        //这是流式处理的环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment stableEnv = TableEnvironment.getTableEnvironment(env);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

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

        DataSource<Tuple3<Long, String, Integer>> orderSource = env.fromCollection(dataList);

        Table table = tableEnv.fromDataSet(orderSource,"user,product,amout");

        Table user = table.groupBy("user").select("user,product.count as cnt,sum(amout) as sum");

        tableEnv.toDataSet(user, Row.class).print();

        env.execute("TableOperatorBach");

    }
}
