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
public abstract class SimulateBolt extends BaseRichBolt {

    private transient Random rand;
    private double mu;
    protected transient OutputCollector collector;
    protected transient ExecuteMetric executeMetric;

    public SimulateBolt(double mu) {
        this.mu = mu;
    }

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector outputCollector) {
        this.collector = outputCollector;
        rand = new Random();
        executeMetric = context.registerMetric(context.getThisComponentId() + "-exe", new ExecuteMetric(), 10);
    }

    @Override
    public void execute(Tuple tuple) {
        double inter = (-Math.log(rand.nextDouble()) * 1000.0 / mu);
        Utils.sleep((long) inter);
        // metric format
        // key is sentence id
        // value format is <SourceComponent>:<SourceStreamId>,<sleepInterval>,<leaveTime>
        StringBuilder value = new StringBuilder();
        value.append(tuple.getSourceComponent()).append(':');
        value.append(tuple.getSourceStreamId()).append(',');
        value.append(inter).append(',').append(System.currentTimeMillis());
        executeMetric.addMetric(tuple.getString(0), value.toString());
    }
}
