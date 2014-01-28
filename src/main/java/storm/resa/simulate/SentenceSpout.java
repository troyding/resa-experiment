package storm.resa.simulate;

import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.resa.spout.RedisQueueSpout;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class SentenceSpout extends RedisQueueSpout {

    private transient long count = 0;
    private String spoutIdPrefix = "s-";
    private transient TupleCompletedMetric completedMetric;

    public SentenceSpout(String host, int port, String queue) {
        super(host, port, queue);
    }

    @Override
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        super.open(map, context, collector);
        spoutIdPrefix = spoutIdPrefix + context.getThisTaskId() + '-';
        completedMetric = context.registerMetric("tuple-completed", new TupleCompletedMetric(), 60);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sid", "sentence"));
    }

    @Override
    protected void emitData(Object data) {
        String id = spoutIdPrefix + count;
        count++;
        collector.emit(new Values(id, data), id);
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