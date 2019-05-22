package com.bigdata.batch.cache;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class CacheDemo {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //注意，在windows，一定要以file:///开头，文件之间要是"/"
        env.registerCachedFile("file:///D:/workspace/filink-project/src/main/resources/filecache","cityFile");

        DataSet<Tuple3<String, String, String>> types = env.readCsvFile("D:\\workspace\\filink-project\\src\\main\\resources\\test.csv")
                .types(String.class, String.class, String.class);

        //distinct的时候，不加参数，默认是全部，加的话，是对那个字段去重
        types.distinct(0).map(new myMapPartition()).print();
    }

    public static class myMapPartition extends RichMapFunction<Tuple3<String, String, String>, Tuple4<String, String, String, String>> {

        Map<String,String> cityMap = new HashMap<>();
        @Override
        public void open(Configuration parameters) throws Exception {
            //可以看见，分布式缓存的文件是作为 一个File缓存的
            File cityFile = getRuntimeContext().getDistributedCache().getFile("cityFile");

            BufferedReader bufferedReader = new BufferedReader(new FileReader(cityFile));
            String line = null;
            while ((line=bufferedReader.readLine())!=null){
                String[] split = line.split("=");
                cityMap.put(split[0],split[1]);
            }
        }

        @Override
        public Tuple4<String, String, String, String> map(Tuple3<String, String, String> t) throws Exception {
            return new Tuple4<>(t.f0,t.f1,t.f2,cityMap.get(t.f0));
        }
    }


}
