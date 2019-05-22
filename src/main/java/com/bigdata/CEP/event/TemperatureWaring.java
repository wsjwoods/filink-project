package com.bigdata.CEP.event;


public class TemperatureWaring {

    private int rackID;

    private Double avgTemperature;

    public TemperatureWaring(int rackID, Double avgTemperature) {
        this.rackID = rackID;
        this.avgTemperature = avgTemperature;
    }

    public int getRackID() {
        return rackID;
    }

    public void setRackID(int rackID) {
        this.rackID = rackID;
    }

    public Double getAvgTemperature() {
        return avgTemperature;
    }

    public void setAvgTemperature(Double avgTemperature) {
        this.avgTemperature = avgTemperature;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TemperatureWaring) {
            TemperatureWaring other = (TemperatureWaring) obj;

            return rackID == other.rackID && avgTemperature == other.avgTemperature;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 41 * rackID + Double.hashCode(avgTemperature);
    }
    @Override
    public String toString() {
        return "TemperatureWaring{" +
                "rackID=" + rackID +
                ", avgTemperature=" + avgTemperature +
                '}';
    }
}
