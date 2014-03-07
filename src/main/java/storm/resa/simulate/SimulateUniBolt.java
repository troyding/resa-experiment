package storm.resa.simulate;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Created by ding on 14-1-27.
 */
public abstract class SimulateUniBolt extends BaseRichBolt {

    private transient Random rand;
    private double lmu, rmu;
    protected transient OutputCollector collector;
    protected transient ExecuteMetric executeMetric;

    public SimulateUniBolt(double lmu, double rmu) {
        this.lmu = lmu;
        this.rmu = rmu;
    }

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector outputCollector) {
        this.collector = outputCollector;
        rand = new Random();
        executeMetric = context.registerMetric(context.getThisComponentId() + "-exe", new ExecuteMetric(), 10);
    }

    @Override
    public void execute(Tuple tuple) {
    	long arrivalTime = System.currentTimeMillis();
    	double inter = 1000.0 / ((1.0 - rand.nextDouble()) * (rmu - lmu) + lmu);
        Utils.sleep((long) inter);
        // metric format
        // key is sentence id
        // value format is <SourceComponent>:<SourceStreamId>,<sleepInterval>,<leaveTime>
        StringBuilder value = new StringBuilder();
        value.append(tuple.getSourceComponent()).append(':');
        value.append(tuple.getSourceStreamId()).append(',');
        value.append(arrivalTime).append(',').append(inter).append(',').append(System.currentTimeMillis());
        executeMetric.addMetric(tuple.getString(0), value.toString());
    }
}
