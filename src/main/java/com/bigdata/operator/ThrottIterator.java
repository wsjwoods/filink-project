package com.bigdata.operator;

import java.io.Serializable;
import java.util.Iterator;



public class ThrottIterator<T> implements Iterator<T>,Serializable {

    private final Iterator<T> source;

    private final long sleepBatchSize;
    private final long sleepBathTime;

    private long lastBatchTime;
    private long num;

    public ThrottIterator(Iterator<T> source, long elementPerSec) throws Exception {
        this.source = source;
        if(elementPerSec>=100){
            this.sleepBatchSize = elementPerSec/20;
            this.sleepBathTime = 50;
        }else if(elementPerSec>=1){
            this.sleepBatchSize=1;
            this.sleepBathTime = 1000/elementPerSec;
        }else {
            throw new Exception("elementPerSec must be big than 0");
        }
    }

    @Override
    public boolean hasNext() {
        return source.hasNext();
    }

    @Override
    public T next() {
        if(lastBatchTime>0){
            if(++num>=sleepBatchSize){
                num=0;

                final  long now = System.currentTimeMillis();
                final long elapsed = now - lastBatchTime;
                if(elapsed < sleepBathTime){
                    try {
                        Thread.sleep(sleepBathTime-elapsed);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                lastBatchTime = now;

            }
        }else{
            lastBatchTime = System.currentTimeMillis();
        }
        return source.next();
    }
}
