package com.bigdata.CEP;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

/**
 * 场景:
 * 用户id,时间戳，用户事件类型
 * 判断一个用户在一段时间内登陆事件的次数
 * 如果超出一分钟三次，那么，我们就要对他进行提醒
 */
public class FlinkCEPExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<Integer, Long, String>> userEvent = env.fromElements(Tuple3.of(1, 1000L, "login"),
                Tuple3.of(2, 3000L, "mai"),
                Tuple3.of(1, 2000L, "login"),
                Tuple3.of(1, 3000L, "login"),
                Tuple3.of(1, 3000L, "fukuan")).keyBy(0);

        Pattern<Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> times = Pattern.<Tuple3<Integer, Long, String>>begin("first")
                .where(new IterativeCondition<Tuple3<Integer, Long, String>>() {
                    @Override
                    public boolean filter(Tuple3<Integer, Long, String> t, Context<Tuple3<Integer, Long, String>> context) throws Exception {
                        if (t.f2.equals("login")) {
                            return true;
                        }
                        return false;
                    }
                }).times(4);

        PatternStream<Tuple3<Integer, Long, String>> pattern = CEP.pattern(userEvent, times);

        pattern.select(new PatternSelectFunction<Tuple3<Integer,Long,String>, Object>() {
            public Object select(Map<String, List<Tuple3<Integer, Long, String>>> map) throws Exception {
                List<Tuple3<Integer, Long, String>> first = map.get("first");
                return "user: "+first.get(0).f0+" login times is： "+first.size();
            }
        }).print();

        env.execute("FlinkCEPExample");

    }
}
