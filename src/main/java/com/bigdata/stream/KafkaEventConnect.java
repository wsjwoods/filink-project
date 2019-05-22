package com.bigdata.stream;

import com.bigdata.bean.KafkaEvent;
import com.bigdata.bean.KafkaEventSchem;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class KafkaEventConnect {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000L);
        String checkDir = "file:///D:\\workspace\\filink-project\\result\\check-dir";
        env.setStateBackend(new FsStateBackend(checkDir,true));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","node1:9092");
        properties.setProperty("group.id","testtest");

        Map<KafkaTopicPartition,Long> topicOffset = new HashMap<>();
        topicOffset.put(new KafkaTopicPartition("wordfre",0),5L);
        topicOffset.put(new KafkaTopicPartition("wordfre",1),5L);
        topicOffset.put(new KafkaTopicPartition("wordfre",2),5L);

        FlinkKafkaConsumer010<KafkaEvent> consumer = new FlinkKafkaConsumer010<>("wordfre", new KafkaEventSchem(), properties);
        DataStream<KafkaEvent> input = env
                .addSource(
                        new FlinkKafkaConsumer010<>(
                                "wordfre",
                                new KafkaEventSchem(),
                                properties)
                                //.setStartFromGroupOffsets()
                                //.setStartFromEarliest()
                                .setStartFromSpecificOffsets(topicOffset)
                                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<KafkaEvent>() {

                                    @Override
                                    public long extractAscendingTimestamp(KafkaEvent element) {
                                        return element.getTimestamp();
                                    }
                                })).keyBy("word")
                .map(new RichMapFunction<KafkaEvent, KafkaEvent>() {

                    private ValueState<Integer> currentValue;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        currentValue = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("currentValue", Integer.class));
                    }

                    @Override
                    public KafkaEvent map(KafkaEvent value) throws Exception {
                        Integer value1 = currentValue.value();
                        if (value1 == null) {
                            value1 = 0;
                        }
                        value1 += value.getFrequency();
                        currentValue.update(value1);
                        return new KafkaEvent(value.getWord(), value1, value.getTimestamp());
                    }
                });


        FlinkKafkaProducer010<KafkaEvent> wordSink = new FlinkKafkaProducer010<>("wordresu", new KafkaEventSchem(), properties);
        input.addSink(wordSink);

        env.execute("KafkaEventConnect");


    }
}
