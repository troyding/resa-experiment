package storm.resa.metric;

import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import org.apache.log4j.Logger;
import org.json.simple.JSONValue;
import redis.clients.jedis.Jedis;

import java.util.*;

/**
 * Write metrics to redis server.
 * <p>
 * Created by ding on 13-12-11.
 */
public class RedisMetricsCollector extends ConsumerBase {

    public static class QueueElement {
        public final String queueName;
        public final String data;

        public QueueElement(String queueName, String data) {
            this.queueName = queueName;
            this.data = data;
        }
    }

    public static final String REDIS_HOST = "storm.resa.metrics.redis.host";
    public static final String REDIS_PORT = "storm.resa.metrics.redis.port";
    public static final String REDIS_QUEUE_NAME = "storm.resa.metrics.redis.queue-name";

    private static final Logger LOG = Logger.getLogger(RedisMetricsCollector.class);

    private transient Jedis jedis;
    private String jedisHost;
    private int jedisPort;
    protected String queueName;

    @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context,
                        IErrorReporter errorReporter) {
        super.prepare(stormConf, registrationArgument, context, errorReporter);
        Map<String, Object> regArgu = (Map<String, Object>) registrationArgument;
        jedisHost = (String) regArgu.get(REDIS_HOST);
        jedisPort = ((Number) regArgu.get(REDIS_PORT)).intValue();
        queueName = (String) regArgu.get(REDIS_QUEUE_NAME);
        // queue name is not exist, use topology id as default
        if (queueName == null) {
            queueName = context.getStormId();
        }
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
        List<QueueElement> data = dataPoints2QueueElement(taskInfo, dataPoints);
        if (data == null) {
            return;
        }
        LOG.info("data size is " + data.size());
        //push to redis
        for (QueueElement e : data) {
            try {
                getJedisInstance().rpush(e.queueName, e.data);
            } catch (Exception e1) {
                LOG.info("push data to redis failed", e1);
                closeJedis();
            }
        }
    }

    protected List<QueueElement> dataPoints2QueueElement(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
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
        return Collections.singletonList(new QueueElement(queueName, sb.toString()));
    }


    @Override
    public void cleanup() {
        closeJedis();
    }
}