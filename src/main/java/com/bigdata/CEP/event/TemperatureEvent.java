package com.bigdata.CEP.event;

/**
 * 服务器的存储，其实是放在每一个机架上的，所以，我们模拟的时候，也模拟一下机架
 */
public class TemperatureEvent extends MonitorEvent{

    private Double temperature;


    public TemperatureEvent(int rackID,Double temperature) {
        super(rackID);
        this.temperature = temperature;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "TemperatureEvent{" +
                "rackID=" +getRackID()+
                "temperature=" + temperature +
                '}';
    }
}
