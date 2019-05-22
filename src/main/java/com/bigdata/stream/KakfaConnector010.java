package com.bigdata.stream;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;


public class KakfaConnector010 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","node1:9092");
        properties.setProperty("group.id","test");

        //DataStream<String> test = env.addSource(new FlinkKafkaConsumer010<String>("wordfre", new SimpleStringSchema(), properties));
        FlinkKafkaConsumer010<String> wordfre = new FlinkKafkaConsumer010<>(java.util.regex.Pattern.compile("test-[0-9]"), new SimpleStringSchema(), properties);
        wordfre.setStartFromLatest();

        DataStream<String> consume = env.addSource(wordfre);


        consume.print();

        env.execute("KakfaConnector010");

    }
}
