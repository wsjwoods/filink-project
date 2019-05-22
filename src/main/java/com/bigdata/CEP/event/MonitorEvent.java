package com.bigdata.CEP.event;

public abstract class MonitorEvent {

    private int rackID;

    public MonitorEvent(int rackID) {
        this.rackID = rackID;
    }

    public int getRackID() {
        return rackID;
    }

    public void setRackID(int rackID) {
        this.rackID = rackID;
    }
}
