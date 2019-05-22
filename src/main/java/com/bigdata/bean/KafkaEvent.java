package com.bigdata.bean;

public class KafkaEvent {

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public KafkaEvent() {}

    public KafkaEvent(String word, int frequency, long timestamp) {
        this.word = word;
        this.frequency = frequency;
        this.timestamp = timestamp;
    }

    private String word;

    @Override
    public String toString() {
        return word +","+ frequency +","+timestamp;
    }

    private int frequency;
    private long timestamp;

    public static KafkaEvent fromString(String kafkaStr){
        String[] split = kafkaStr.split(",");
        if(split.length>2){
            return new KafkaEvent(split[0],Integer.valueOf(split[1]),Long.valueOf(split[2]));
        }else {
            return new KafkaEvent("",0,0L);
        }

    }
}
