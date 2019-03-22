package com.calabar.flinkDemo.stream;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class MyNoParalleStrSource implements SourceFunction<String> {

    private long count = 1L;
    private String str = "test1,test2,";
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while(isRunning) {
            sourceContext.collect(str+count);
            count ++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
