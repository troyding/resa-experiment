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
public class TomExpServiceWCTopology2 {


    public static void main(String[] args) throws Exception {

        Config conf = ConfigUtil.readConfig(new File(args[1]));

        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[1]);
        }

        TopologyBuilder builder = new TopologyBuilder();

        int numWorkers = ConfigUtil.getInt(conf, "a2-worker.count", 1);
        int numAckers = ConfigUtil.getInt(conf, "a2-acker.count", 1);

        conf.setNumWorkers(numWorkers);
        conf.setNumAckers(numAckers);

        String host = (String) conf.get("redis.host");
        int port = ConfigUtil.getInt(conf, "redis.port", 6379);

        if (ConfigUtil.getBoolean(conf, "spout.redis", false) == false) {
            builder.setSpout("spout", new RandomSentenceSpout(), ConfigUtil.getInt(conf, "a2-spout.parallelism", 1));
        } else {
            String queue = (String) conf.get("a2-redis.queue");
            ///by default, prob = 1.0, is becoming the original two-step topology
            builder.setSpout("spout", new SentenceSpoutT2(host, port, queue, ConfigUtil.getDouble(conf, "a2-spout.prob", 1.0)),
                    ConfigUtil.getInt(conf, "a2-spout.parallelism", 1));
        }

        ///builder.setBolt("split", new SplitSentence(ConfigUtil.getDouble(conf, "a2-split.mu", 1.0)),
        ///        ConfigUtil.getInt(conf, "a2-split.parallelism", 1)).shuffleGrouping("say");
        ///builder.setBolt("counter", new WordCounter(ConfigUtil.getDouble(conf, "a2-counter.mu", 1.0)),
        ///		ConfigUtil.getInt(conf, "a2-counter.parallelism", 1)).shuffleGrouping("split");
                //ConfigUtil.getInt(conf, "counter.parallelism", 1)).fieldsGrouping("split", new Fields("word"));

        builder.setBolt("split-BP", new SplitSentence(ConfigUtil.getDouble(conf, "a2-split.mu", 1.0)),
                ConfigUtil.getInt(conf, "a2-split.parallelism", 1)).shuffleGrouping("spout", "Bolt-P");
        
        ///The last bolt accept all tuples from split-BP and partial stream (Pnot) directly from spout.
        builder.setBolt("counter-BPnot", new WordCounter(ConfigUtil.getDouble(conf, "a2-counter.mu", 1.0)),
        		ConfigUtil.getInt(conf, "a2-counter.parallelism", 1))
        		.shuffleGrouping("spout", "Bolt-Pnot")
        		.shuffleGrouping("split-BP");
                
        Map<String, Object> metricsConsumerArgs = new HashMap<String, Object>();
        metricsConsumerArgs.put(RedisMetricsCollector.REDIS_HOST, host);
        metricsConsumerArgs.put(RedisMetricsCollector.REDIS_PORT, port);
        metricsConsumerArgs.put(ConsumerBase.METRICS_NAME, Arrays.asList("tuple-completed"));
        String queueName = conf.get("a2-metrics.output.queue-name").toString();
        if (queueName != null) {
            metricsConsumerArgs.put(RedisMetricsCollector.REDIS_QUEUE_NAME, queueName);
        }
        conf.registerMetricsConsumer(MetricsCollector.class, metricsConsumerArgs, 1);

        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
}
