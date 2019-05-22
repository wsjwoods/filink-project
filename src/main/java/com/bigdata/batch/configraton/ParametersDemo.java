package com.bigdata.batch.configraton;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

public class ParametersDemo {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Integer> integerData= env.fromElements(1, 2, 3);

        Configuration config = new Configuration();
        config.setInteger("limit",2);

        integerData.filter(new RichFilterFunction<Integer>() {

            private int limit;
            @Override
            public void open(Configuration parameters) throws Exception {
                limit = parameters.getInteger("limit",0);
            }

            @Override
            public boolean filter(Integer integer) throws Exception {
                return integer > limit;
            }
        }).withParameters(config).print();

    }
}
