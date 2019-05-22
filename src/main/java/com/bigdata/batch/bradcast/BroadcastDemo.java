package com.bigdata.batch.bradcast;

import com.bigdata.batch.source.People;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.util.List;


public class BroadcastDemo {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //这个就是我们要广播出去的黑名单的数据
        DataSet<People> zhaosiData = env.fromElements(new People("zhaosi", 29, "1224"));


        //假设我广播出去一份黑名单数据集
        DataSet<People> peopleDataSource = env.readCsvFile("D:\\workspace\\filink-project\\src\\main\\resources\\test.csv")
              .pojoType(People.class, "name", "age", "phone");

        //我们选择的匿名内部类是RichFilterFunction而不是FilterFunction，原因：
        //RichFilterFunction除了提供原有的FilterFunction方法之外
        //还提供le open、close、getRuntimeContext和setRuntimeContext等方法
        //这些方法可以用于参数化函数的创建
        //访问广播变量以及访问运行时信息以及迭代的相关信息
        peopleDataSource.filter(new RichFilterFunction<People>() {

            List<People> blackListName = null;
            @Override
            public void open(Configuration parameters) throws Exception {
                blackListName = getRuntimeContext().getBroadcastVariable("blackListName");
            }

            @Override
            public boolean filter(People People) throws Exception {
                for (People eachp:blackListName) {
                    return !People.name.equals(eachp.name);
                }
                return false;
            }
        }).withBroadcastSet(zhaosiData,"blackListName").print();

    }
}
