package com.bigdata.operator;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;


public class WindowJoinSampleData {

    //生成的第一个流  name grade
    //生成的第二个流  name salary
    static final String[] NAMES = {"tom","jerry","alice","bob","john","garce"};

    static final int GRADE_COUNT = 5 ;
    static final int SALARY_MAX = 10000;

    public static class  GradeSource implements Iterator<Tuple2<String,Integer>>,Serializable{

        private final Random rnd = new Random(hashCode());

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Tuple2<String, Integer> next() {
            return new Tuple2<>(NAMES[rnd.nextInt(NAMES.length)],rnd.nextInt(GRADE_COUNT)+1);
        }

        public static DataStream<Tuple2<String,Integer>> getSource(
                StreamExecutionEnvironment env,long rate) throws Exception {
            return env.fromCollection(new ThrottIterator<>(new GradeSource(),rate), TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
            }));
        }
    }

    public static class  SalarySource implements Iterator<Tuple2<String,Integer>>,Serializable{

        private final Random rnd = new Random(hashCode());

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Tuple2<String, Integer> next() {
            return new Tuple2<>(NAMES[rnd.nextInt(NAMES.length)],rnd.nextInt(SALARY_MAX)+1);
        }

        public static DataStream<Tuple2<String,Integer>> getSource(
                StreamExecutionEnvironment env,long rate) throws Exception {
            return env.fromCollection(new ThrottIterator<>(new SalarySource(),rate), TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
            }));
        }
    }
}
