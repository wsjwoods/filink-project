package com.learn.stream.oneday;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class SimpleSourceFunction extends RichParallelSourceFunction {

    private long num = 0;
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext ctx) throws Exception {
        while (isRunning) {
            ctx.collect(num);
            num++;
            Thread.sleep(3000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
