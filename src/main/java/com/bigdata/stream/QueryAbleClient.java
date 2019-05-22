package com.bigdata.stream;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.util.concurrent.CompletableFuture;



public class QueryAbleClient {

    public static void main(String[] args) throws Exception{
        QueryableStateClient client = new QueryableStateClient("hostname", 1234);

        ValueStateDescriptor<Tuple2<Long,Long>> descriptor =
                new ValueStateDescriptor<Tuple2<Long, Long>>(
                        "averade",
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                        })
                );

        CompletableFuture<ValueState<Tuple2<Long, Long>>> kvStateResult = client.
                getKvState(new JobID("112131".getBytes()), "query-name",
                        12L, BasicTypeInfo.LONG_TYPE_INFO, descriptor);

        Tuple2<Long, Long> value = kvStateResult.get().value();
        System.out.println(value.f0+"\t"+value.f1);
    }
}
