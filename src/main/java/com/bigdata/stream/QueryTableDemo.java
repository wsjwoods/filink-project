package com.bigdata.stream;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


public class QueryTableDemo {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        QueryableStateClient client = new QueryableStateClient("",12);

// the state descriptor of the state to be fetched.
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<Tuple2<Long, Long>>(
                        "average",
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}));

        CompletableFuture<ValueState<Tuple2<Long, Long>>> resultFuture =
                client.getKvState(new JobID(("aaa".getBytes())), "query-name", 12L, BasicTypeInfo.LONG_TYPE_INFO, descriptor);


// now handle the returned value
        Tuple2<Long, Long> value = resultFuture.get().value();
        System.out.println(value.f0+"\t"+value.f1);

//        resultFuture.thenAccept(response => {
//            try {
//                response.value();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        });
    }
}
