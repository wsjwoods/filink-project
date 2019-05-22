package com.bigdata.CEP.source;

import com.bigdata.CEP.event.MonitorEvent;
import com.bigdata.CEP.event.PowerEvent;
import com.bigdata.CEP.event.TemperatureEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class MonitoringEventSource extends RichParallelSourceFunction<MonitorEvent>{

    private boolean running = true;

    private final int maxRackID;

    private final long pause;

    private final double temperatureRation;

    private final double temperatureStd;

    private final double temperaturemean;

    private final double powerStd;

    private final double powermean;

    private int shard;

    private int offset;

    private Random random;

    public MonitoringEventSource(int maxRackID, long pause, double temperatureRation, double temperatureStd, double temperaturemean, double powerStd, double powermean) {
        this.maxRackID = maxRackID;
        this.pause = pause;
        this.temperatureRation = temperatureRation;
        this.temperatureStd = temperatureStd;
        this.temperaturemean = temperaturemean;
        this.powerStd = powerStd;
        this.powermean = powermean;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();

        offset = (int) ((double)maxRackID/numberOfParallelSubtasks*indexOfThisSubtask);

        shard = (int)((double)maxRackID/numberOfParallelSubtasks*(indexOfThisSubtask+1))-offset;

        random = new Random();
    }

    @Override
    public void run(SourceContext<MonitorEvent> ctx) throws Exception {
        while (running){
            MonitorEvent monitorEvent;

            int rackID = random.nextInt(shard)+offset;

            if(random.nextDouble() < temperatureRation){
                double temperature = random.nextGaussian()*temperatureStd+temperaturemean;
                monitorEvent = new TemperatureEvent(rackID,temperature);
            }else{
                double power = random.nextGaussian()*powerStd+powermean;
                monitorEvent = new PowerEvent(rackID,power);
            }

            ctx.collect(monitorEvent);

            Thread.sleep(pause);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
