package storm.resa.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by ding on 14-1-16.
 */
public class RedisQueueSpout extends BaseRichSpout {

    public static final String OUTPUT_FIELD_NAME = "text";
    protected SpoutOutputCollector collector;

    private String queue;
    private String host;
    private int port;
    private transient Jedis jedis;

    public RedisQueueSpout(String host, int port, String queue) {
        this.host = host;
        this.port = port;
        this.queue = queue;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(OUTPUT_FIELD_NAME));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        jedis = new Jedis(host, port);
    }

    @Override
    public void close() {
        jedis.disconnect();
    }

    @Override
    public void nextTuple() {
        Object text = jedis.lpop(queue);
        if (text != null) {
            collector.emit(Arrays.asList(text), text);
        }
    }

}
