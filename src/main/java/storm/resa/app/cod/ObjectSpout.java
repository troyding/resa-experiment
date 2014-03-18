package storm.resa.app.cod;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import storm.resa.spout.RedisQueueSpout;

/**
 * Created by ding on 14-3-14.
 */
public class ObjectSpout extends RedisQueueSpout {

    public static final String ID_FILED = "id";
    public static final String VECTOR_FILED = "vector";
    public static final String TIME_FILED = "time";

    public ObjectSpout(String host, int port, String queue) {
        super(host, port, queue);
    }

    @Override
    protected void emitData(Object data) {
        String vectorStr = (String) data;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(ID_FILED, VECTOR_FILED, TIME_FILED));
    }
}
