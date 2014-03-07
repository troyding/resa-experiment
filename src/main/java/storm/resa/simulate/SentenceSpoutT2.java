package storm.resa.simulate;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.resa.spout.RedisQueueSpout;

import java.util.Map;
import java.util.Random;

public class SentenceSpoutT2 extends RedisQueueSpout {

    private transient long count = 0;
    private String spoutIdPrefix = "T2-s-";
    private transient TupleCompletedMetric completedMetric;
    private transient Random rand;
    private double p;

    public SentenceSpoutT2(String host, int port, String queue, double p) {
        super(host, port, queue);
        this.p = p;
    }

    @Override
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        super.open(map, context, collector);
        spoutIdPrefix = spoutIdPrefix + context.getThisTaskId() + '-';
        completedMetric = context.registerMetric("tuple-completed", new TupleCompletedMetric(), 60);
        rand = new Random();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        ///declarer.declare(new Fields("sid", "sentence"));        
        declarer.declareStream("Bolt-P", new Fields("sid", "sentence"));
        declarer.declareStream("Bolt-Pnot", new Fields("sid", "sentence"));
    }

    @Override
    protected void emitData(Object data) {
        String id = spoutIdPrefix + count;
        count++;        
        
        double prob = rand.nextDouble();
        if (prob < this.p){
        	collector.emit("Bolt-P", new Values(id, data), id);
        }
        else{
        	collector.emit("Bolt-Pnot", new Values(id, data), id);
        }
        ///collector.emit(new Values(id, data), id);
        completedMetric.tupleStarted(id);
    }

    @Override
    public void ack(Object msgId) {
        completedMetric.tupleCompleted((String) msgId);
    }

    @Override
    public void fail(Object msgId) {
        completedMetric.tupleFailed((String) msgId);
    }
}