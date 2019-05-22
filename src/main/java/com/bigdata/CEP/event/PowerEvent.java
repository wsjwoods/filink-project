package com.bigdata.CEP.event;


public class PowerEvent extends MonitorEvent{

    private double power;

    public PowerEvent(int rackID,double power) {
        super(rackID);

        this.power=power;
    }

    public double getPower() {
        return power;
    }

    public void setPower(double power) {
        this.power = power;
    }
}
