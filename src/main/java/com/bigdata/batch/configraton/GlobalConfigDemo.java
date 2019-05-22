package com.bigdata.batch.configraton;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

/**
 * GlobalConfig和params的不一样的地方在于，后者只是在局部代码中设置的config，并没有影响到env，所有，
 * 在代码的后面要加上withParameters(config)，但是GlobalConfig不同的是，它改变了env的环境里面的config，
 * 所以，在代码后面，不需要显式的指定加上config’
 */
public class GlobalConfigDemo {

    public static void main(String[] args) throws Exception {

        Configuration config = new Configuration();
        config.setString("blackList","zhaosi");

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //这里把我们的config添加到全局(env)的config中
        env.getConfig().setGlobalJobParameters(config);

        DataSet<Tuple3<String, String, String>> types = env.readCsvFile("D:\\workspace\\filink-project\\src\\main\\resources\\test.csv")
                .types(String.class, String.class, String.class);

        types.distinct(0).filter(new RichFilterFunction<Tuple3<String, String, String>>() {

            String blackPeople = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ExecutionConfig.GlobalJobParameters globalJobParameters = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

                Configuration gloalConfig = (Configuration) globalJobParameters;
                blackPeople = gloalConfig.getString("blackList","liuliu");

            }

            @Override
            public boolean filter(Tuple3<String, String, String> t) throws Exception {

                return !t.f0.equals(blackPeople);
            }
        }).print();


    }

}
