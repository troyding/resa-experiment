package storm.resa.measure;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by ding on 14-1-27.
 */
public class MeasurableBolt implements IRichBolt {

    private transient ExecuteMetric executeMetric;
    private IRichBolt delegate;
    private TraceIdGenerator.OfBolt traceIdGenerator;

    public MeasurableBolt(IRichBolt delegate, TraceIdGenerator.OfBolt generator) {
        this.delegate = delegate;
        this.traceIdGenerator = generator;
    }

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector outputCollector) {
        executeMetric = context.registerMetric(context.getThisComponentId() + "-exe", new ExecuteMetric(), 10);
        delegate.prepare(map, context, outputCollector);
    }

    @Override
    public void execute(Tuple tuple) {
        String id = traceIdGenerator.apply(tuple);
        long arrivalTime = System.currentTimeMillis();
        // double inter = (-Math.log(rand.nextDouble()) * 1000.0 / mu);
        // Utils.sleep((long) inter);
        delegate.execute(tuple);
        // metric format
        // key is sentence id
        // value format is <SourceComponent>:<SourceStreamId>,<arrivalTime>,<leaveTime>
        StringBuilder value = new StringBuilder();
        value.append(tuple.getSourceComponent()).append(':');
        value.append(tuple.getSourceStreamId()).append(',');
        value.append(arrivalTime).append(',').append(System.currentTimeMillis());
        executeMetric.addMetric(id, value.toString());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        delegate.declareOutputFields(declarer);
    }

    @Override
    public void cleanup() {
        delegate.cleanup();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return delegate.getComponentConfiguration();
    }
}
