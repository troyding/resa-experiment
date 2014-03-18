package storm.resa.app.cod;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.resa.spout.RedisQueueSpout;

import java.util.Arrays;

/**
 * Created by ding on 14-3-14.
 */
public class ObjectSpout extends RedisQueueSpout {

    public static final String ID_FILED = "id";
    public static final String VECTOR_FILED = "vector";
    public static final String TIME_FILED = "time";

    private final int objectCount;

    public ObjectSpout(String host, int port, String queue, int objectCount) {
        super(host, port, queue);
        this.objectCount = objectCount;
    }

    @Override
    protected void emitData(Object data) {
        String[] tmp = ((String) data).split("|");
        double[] v = Arrays.stream(tmp[2].split(",")).mapToDouble((str) -> Double.parseDouble(str)).toArray();
        int objId = (int) (Long.parseLong(tmp[0]) % objectCount);
        collector.emit(new Values(objId, v, Long.parseLong(tmp[1])));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(ID_FILED, VECTOR_FILED, TIME_FILED));
    }
}
