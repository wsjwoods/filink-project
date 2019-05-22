package com.bigdata.tableAndsql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.serialization.TypeInformationKeyValueSerializationSchema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;


public class ZDKafkaCsvTableSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment stableEnv = TableEnvironment.getTableEnvironment(env);

        String[] names = {"name","local","phone"};
        TypeInformation[] types = {Types.STRING,Types.STRING,Types.STRING};
        TypeInformation<Row> rowTypeInformation = Types.ROW_NAMED(names, types);

        String topic = "test12";
        Properties pro = new Properties();
        pro.setProperty("bootstrap.servers","node1:9092");
        pro.setProperty("group.id","ttest");
        pro.setProperty("zookeeper.connect","node1:2181");

        Kafkatablesource kafkatablesource = new Kafkatablesource(topic, pro, new KafkaCsvSourceSchem(rowTypeInformation), rowTypeInformation);

        //把你自定义数据源(kakfa)表定义成一张表
        //接下来的处理，和前面处理的是一模一样
        stableEnv.registerTableSource("csvtable",kafkatablesource);

        env.execute("ZDKafkaCsvTableSource");
    }
}
