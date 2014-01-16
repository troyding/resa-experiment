package storm.resa.metric;

import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import org.apache.log4j.Logger;
import org.json.simple.JSONValue;
import redis.clients.jedis.Jedis;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Write metrics to redis server.
 * <p/>
 * Created by ding on 13-12-11.
 */
public class RedisMetricsCollector extends ConsumerBase {

    private static final Logger LOG = Logger.getLogger(RedisMetricsCollector.class);

    private Jedis jedis;
    private String jedisHost;
    private int jedisPort;
    private String topologyId;

    @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context,
                        IErrorReporter errorReporter) {
        super.prepare(stormConf, registrationArgument, context, errorReporter);
        Map<String, Object> regArgu = (Map<String, Object>) registrationArgument;
        jedisHost = (String) regArgu.get("storm.resa.metrics.redis.host");
        jedisPort = ((Number) regArgu.get("storm.resa.metrics.redis.host")).intValue();
        this.topologyId = context.getStormId();
        LOG.info("Write metrics to redis server " + jedisHost + ":" + jedisPort);
    }

    /* get a jedis instance, create a one if necessary */
    private Jedis getJedisInstance() {
        if (jedis == null) {
            jedis = new Jedis(jedisHost, jedisPort);
            LOG.info("connecting to redis server " + jedisHost);
        }
        return jedis;
    }

    private void closeJedis() {
        if (jedis != null) {
            try {
                LOG.info("disconnecting redis server " + jedisHost);
                jedis.disconnect();
            } catch (Exception e) {
            }
            jedis = null;
        }
    }

    @Override
    protected void handleSelectedDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        //data format is "[srcComponentId-taskId]:timestamp:data point json"
        StringBuilder sb = new StringBuilder();
        sb.append('[').append(taskInfo.srcComponentId).append('-').append(taskInfo.srcTaskId).append("]:");
        sb.append(taskInfo.timestamp).append(':');
        //convert data points to json string
        Map<String, Object> metrics = new HashMap<String, Object>((int) (dataPoints.size() / 0.75f) + 1, 0.75f);
        for (DataPoint dataPoint : dataPoints) {
            metrics.put(dataPoint.name, dataPoint.value);
        }
        sb.append(JSONValue.toJSONString(metrics));
        //push to redis
        try {
            getJedisInstance().rpush(topologyId, sb.toString());
        } catch (Exception e) {
            closeJedis();
        }
    }

    @Override
    public void cleanup() {
        closeJedis();
    }
}