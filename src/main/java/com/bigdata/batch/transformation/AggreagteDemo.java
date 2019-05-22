package com.bigdata.batch.transformation;

import com.bigdata.batch.source.People;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class AggreagteDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> stringDataSource = env.readTextFile("E:\\filink-project\\src\\main\\resources\\wordcount");

        DataSet<Tuple2<String, Integer>> mapPartitionOperator = stringDataSource.mapPartition(new MapPartitionFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void mapPartition(Iterable<String> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : iterable) {
                    for (String s : word.split(" ")) {
                        collector.collect(new Tuple2<>(s, 1));
                    }
                }
            }
        });
        mapPartitionOperator.print();
        System.out.println("--------------------------------------------");
        mapPartitionOperator.groupBy(0).aggregate(Aggregations.SUM,1).print();
        System.out.println("--------------------------------------------");
        mapPartitionOperator.groupBy(0).aggregate(Aggregations.MAX,1).print();
        System.out.println("--------------------------------------------");
        mapPartitionOperator.groupBy(0).aggregate(Aggregations.SUM, 1).max(1).print();
        mapPartitionOperator.groupBy(0).aggregate(Aggregations.SUM, 1).minBy(0, 1).print();

     /*   System.out.println("--------------------------------------------");

        System.out.println(mapPartitionOperator.distinct(0,1).count());

        mapPartitionOperator.distinct(new KeySelector<Tuple2<String,Integer>, Integer>() {
            @Override
            public Integer getKey(Tuple2<String, Integer> t) throws Exception {
                return t.f1;
            }
        });*/

        /*System.out.println("--------------------------------------------");

        DataSource<People> peopleDataSource = env.readCsvFile("E:\\filink-project\\src\\main\\resources\\test.csv")
                .pojoType(People.class, "name", "age", "phone");


        peopleDataSource.distinct("name","age").print();*/


    }
}
