package com.bigdata.batch.iterate;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

/**
 * 我们要做的是一个pi的计算的迭代操作
 */
public class IterateDemo {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        IterativeDataSet<Integer> iterate = env.fromElements(0).iterate(10000);

        DataSet<Integer> map = iterate.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer integer) throws Exception {
                double x = Math.random();//产生一个0-1的数值
                double y = Math.random();
                return integer + ((x * x + y * y) < 1 ? 1 : 0);
            }
        });

        //这个count返回的是落在圆内计数的次数
        DataSet<Integer> count = iterate.closeWith(map);

        count.map(new MapFunction<Integer, Object>() {
            @Override
            public Object map(Integer integer) throws Exception {
                return (integer/(double)10000) *4;
            }
        }).print();


    }
}
