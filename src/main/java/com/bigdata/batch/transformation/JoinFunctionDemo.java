package com.bigdata.batch.transformation;

import com.bigdata.batch.source.People;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

public class JoinFunctionDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<People> peopleDataSource = env.readCsvFile("D:\\workspace\\filink-project\\src\\main\\resources\\test.csv")
                .pojoType(People.class, "name", "age", "phone");

        DataSet<Tuple2<String, Double>> types = env.readCsvFile("D:\\workspace\\filink-project\\src\\main\\resources\\test1.csv")
                .types(String.class, Double.class);

        DataSet<People> nameDistinct = peopleDataSource.distinct("name");

        //tuple  一般是f0 f1
        DataSet<Tuple2<String, Double>> with = nameDistinct.join(types).where("name").equalTo("f0")
                .with(new JoinFunction<People, Tuple2<String, Double>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> join(People people, Tuple2<String, Double> t) throws Exception {
                        System.out.println(people.age+":"+t.f1);
                        return new Tuple2<>(people.name, people.age * t.f1);
                    }
                });

        //with.print();


        System.out.println("join的优化");
        //如果，你在join的时候，显式的指定，后面的表示小表还是大表，有助于flink代码自身的游湖
        nameDistinct.joinWithTiny(types);
        nameDistinct.joinWithHuge(types);

        nameDistinct.join(types, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE);

        //JoinOperatorBase.JoinHint.BROADCAST_HASH_FIRST
        //意思是，广播第一个输入的并构建hash表，如果第一个表小的话，用这个策略
        //JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND
        //意思是，广播第一个输入的并构建hash表，如果第二个表小的话，用这个策略

        //OPTIMIZER_CHOOSES
        //意思是将选择留给系统，代码自己判断
        //REPARTITION_HASH_FIRST
        //场景，第一个表小于第二个表的时候，用这个比较好
        //REPARTITION_HASH_SECOND
        //场景，第一个表大于第二个表的时候，用这个比较好

        //REPARTITION_SORT_MERGE
        //如果输入已经排序了，肯定用这个策略


        System.out.println("--------------------------------------------");
        //union使用的注意场景
        //两个进行union的必须保持类型一致
        //像这种javabean类型和tuple2类型进行union就是错误的 nameDistinct.union(types);
//        nameDistinct.union(nameDistinct);
//        types.union(types);

        //作用：有助于数据倾斜的处理
        nameDistinct.rebalance();

//        group1     100000  数量过大的话，会严重拖慢任务的进行
//        group2     10
//        group3     20


//        rebalance
//        group1     30000
//        group2     30000
//        group3     30000
        //虽然能够解决数据倾斜，但是这种方法极其的耗费时间，不到万不得已，不建议使用

        nameDistinct.first(2).print();
//
//        People{name='zhangzhang', age=29, phone='1236'}
//        People{name='liuliu', age=34, phone='1237'}
    }
}
