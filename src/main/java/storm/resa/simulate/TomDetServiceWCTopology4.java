package storm.resa.simulate;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.resa.example.wc.RandomSentenceSpout;
import storm.resa.metric.ConsumerBase;
import storm.resa.metric.RedisMetricsCollector;
import storm.resa.util.ConfigUtil;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class TomDetServiceWCTopology4 {


    public static void main(String[] args) throws Exception {

        Config conf = ConfigUtil.readConfig(new File(args[1]));

        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[1]);
        }

        TopologyBuilder builder = new TopologyBuilder();

        int numWorkers = ConfigUtil.getInt(conf, "a4-worker.count", 1);
        int numAckers = ConfigUtil.getInt(conf, "a4-acker.count", 1);

        conf.setNumWorkers(numWorkers);
        conf.setNumAckers(numAckers);

        String host = (String) conf.get("redis.host");
        int port = ConfigUtil.getInt(conf, "redis.port", 6379);

        if (ConfigUtil.getBoolean(conf, "spout.redis", false) == false) {
            builder.setSpout("spout", new RandomSentenceSpout(), ConfigUtil.getInt(conf, "a4-spout.parallelism", 1));
        } else {
        	String queue = (String) conf.get("a4-redis.queue");
            builder.setSpout("spout", new SentenceSpout(host, port, queue), ConfigUtil.getInt(conf, "a4-spout.parallelism", 1));
        }

        builder.setBolt("split", new SplitSentenceDet(ConfigUtil.getDouble(conf, "a4-split.mu", 1.0)),
                ConfigUtil.getInt(conf, "a4-split.parallelism", 1)).shuffleGrouping("spout");
        
        ///The last bolt accept all tuples from split-BP and partial stream (Pnot) directly from spout.
        builder.setBolt("counter", new WordCounterT2Det(ConfigUtil.getDouble(conf, "a4-counter.mu", 1.0), ConfigUtil.getDouble(conf, "a4-loopback.prob", 0.0)),
        		ConfigUtil.getInt(conf, "a4-counter.parallelism", 1))
        		.shuffleGrouping("split")
        		.shuffleGrouping("counter", "LoopBack");
                
        Map<String, Object> metricsConsumerArgs = new HashMap<String, Object>();
        metricsConsumerArgs.put(RedisMetricsCollector.REDIS_HOST, host);
        metricsConsumerArgs.put(RedisMetricsCollector.REDIS_PORT, port);
        metricsConsumerArgs.put(ConsumerBase.METRICS_NAME, Arrays.asList("tuple-completed"));
        String queueName = conf.get("a4-metrics.output.queue-name").toString();
        if (queueName != null) {
            metricsConsumerArgs.put(RedisMetricsCollector.REDIS_QUEUE_NAME, queueName);
        }
        conf.registerMetricsConsumer(MetricsCollector.class, metricsConsumerArgs, 1);

        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
}
