package com.bigdata.CEP.event;


public class TemperatureAlert {

    private int rackID;

    public TemperatureAlert(int rackID) {
        this.rackID = rackID;
    }

    public int getRackID() {
        return rackID;
    }

    public void setRackID(int rackID) {
        this.rackID = rackID;
    }

    @Override
    public String toString() {
        return "TemperatureAlert{" +
                "rackID=" + rackID +
                '}';
    }
}
