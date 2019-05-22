package com.bigdata.stream;

import com.bigdata.operator.WindowJoinSampleData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;


public class FlinkRedisConnector {

    public static void main(String[] args) throws Exception {
        long rate = 3;

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost").setPort(6379).build();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> grade = WindowJoinSampleData.GradeSource.getSource(env, rate);

        grade.addSink(new RedisSink<>(conf, new RedisMapper<Tuple2<String, Integer>>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET,"flink_redis");
            }

            @Override
            public String getKeyFromData(Tuple2<String, Integer> t) {
                return t.f0;
            }

            @Override
            public String getValueFromData(Tuple2<String, Integer> t) {
                return t.f0+"-"+t.f1;
            }
        }));

        env.execute("FlinkRedisConnector");

    }
}
