package com.bigdata.batch.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class MapFunctionDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Tuple3<String, String, String>> types = env.readCsvFile("E:\\filink-project\\src\\main\\resources\\test.csv")
                .types(String.class, String.class, String.class);

        types.map(new MapFunction<Tuple3<String,String,String>, Object>() {
            @Override
            public Object map(Tuple3<String, String, String> t) throws Exception {
                if(t.f0.equals("zhangzhang")){
                    return new Tuple2(t.f0,"u are too old");
                }
                return t;
            }
        }).print();
    }
}
