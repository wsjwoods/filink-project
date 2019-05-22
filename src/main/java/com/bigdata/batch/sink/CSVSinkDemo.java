package com.bigdata.batch.sink;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

public class CSVSinkDemo {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> wordCountSource = env.readTextFile("D:\\workspace\\filink-project\\src\\main\\resources\\wordcount");

        DataSet<Tuple2<String, Integer>> mapPartitionOperator = wordCountSource.mapPartition(new MapPartitionFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void mapPartition(Iterable<String> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String line : iterable) {
                    String[] split = line.split(" ");
                    for (String word : split) {
                        collector.collect(new Tuple2<>(word, 1));
                    }
                }
            }
        });

        DataSet<Tuple2<String, Integer>> aggregate = mapPartitionOperator.groupBy(0).aggregate(Aggregations.SUM, 1);
        //和之前一样，加writemode，设置并行度
        //aggregate.writeAsCsv("D:\\workspace\\filink-project\\result\\wordcount", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        //1.我想换一种分隔符号行不行(现在默认的是","分割)
        //aggregate.writeAsCsv("D:\\workspace\\filink-project\\result\\wordcount","\n","|",FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        //2.我想还要更加灵活的！第一列和第二列之间是"|",第二列和第三列是"?"

//        aggregate.rebalance();

        aggregate.writeAsFormattedText("D:\\workspace\\filink-project\\result\\wordcount",
                FileSystem.WriteMode.OVERWRITE,
                new TextOutputFormat.TextFormatter<Tuple2<String, Integer>>() {
            @Override
            public String format(Tuple2<String, Integer> t) {
                return t.f0+"-"+t.f1;
            }
        }).setParallelism(1);

        env.execute("wordcount csv sink");
    }
}
