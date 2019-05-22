package com.bigdata.tableAndsql;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.Properties;


public class Kafkatablesource implements StreamTableSource<Row>{

    private String topic;

    private Properties properties;

    private DeserializationSchema<Row> deserializationSchema;

    private TypeInformation<Row> typeInfo;

    public Kafkatablesource(String topic, Properties properties, DeserializationSchema<Row> deserializationSchema, TypeInformation<Row> typeInfo) {
        this.topic = topic;
        this.properties = properties;
        this.deserializationSchema = deserializationSchema;
        this.typeInfo = typeInfo;
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        FlinkKafkaConsumer010<Row> kafkaConsumer = getKafkaConsumer(topic, deserializationSchema, properties);
        return execEnv.addSource(kafkaConsumer);
    }

    public FlinkKafkaConsumer010<Row> getKafkaConsumer(String topic,DeserializationSchema<Row> deserializationSchema,Properties properties){
        return new FlinkKafkaConsumer010<Row>(topic,deserializationSchema,properties);
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return typeInfo;
    }

    @Override
    public TableSchema getTableSchema() {
        return null;
    }

    @Override
    public String explainSource() {
        return "";
    }
}
