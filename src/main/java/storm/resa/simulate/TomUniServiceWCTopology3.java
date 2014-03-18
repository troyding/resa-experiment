package storm.resa.simulate;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import storm.resa.app.wc.RandomSentenceSpout;
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
public class TomUniServiceWCTopology3 {


    public static void main(String[] args) throws Exception {

        Config conf = ConfigUtil.readConfig(new File(args[1]));

        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[1]);
        }

        TopologyBuilder builder = new TopologyBuilder();

        int numWorkers = ConfigUtil.getInt(conf, "a3-worker.count", 1);
        int numAckers = ConfigUtil.getInt(conf, "a3-acker.count", 1);

        conf.setNumWorkers(numWorkers);
        conf.setNumAckers(numAckers);

        String host = (String) conf.get("redis.host");
        int port = ConfigUtil.getInt(conf, "redis.port", 6379);

        if (ConfigUtil.getBoolean(conf, "spout.redis", false) == false) {
            builder.setSpout("spout", new RandomSentenceSpout(), ConfigUtil.getInt(conf, "a3-spout.parallelism", 1));
        } else {
            String queue = (String) conf.get("a3-redis.queue");
            ///by default, prob = 1.0, is becoming the original two-step topology
            builder.setSpout("spout", new SentenceSpoutT2(host, port, queue, ConfigUtil.getDouble(conf, "a3-spout.prob", 1.0)),
                    ConfigUtil.getInt(conf, "a3-spout.parallelism", 1));
        }

        ///builder.setBolt("split", new SplitSentence(ConfigUtil.getDouble(conf, "a2-split.mu", 1.0)),
        ///        ConfigUtil.getInt(conf, "a2-split.parallelism", 1)).shuffleGrouping("say");
        ///builder.setBolt("counter", new WordCounter(ConfigUtil.getDouble(conf, "a2-counter.mu", 1.0)),
        ///		ConfigUtil.getInt(conf, "a2-counter.parallelism", 1)).shuffleGrouping("split");
                //ConfigUtil.getInt(conf, "counter.parallelism", 1)).fieldsGrouping("split", new Fields("word"));

        double splitBP_mu = ConfigUtil.getDouble(conf, "a3-splitBP.mu", 1.0);
        double splitBP_lmu = 0.9*splitBP_mu;
        double splitBP_rmu = 1.1*splitBP_mu;
        
        double splitBPnot_mu = ConfigUtil.getDouble(conf, "a3-splitBPnot.mu", 1.0);
        double splitBPnot_lmu = 0.9*splitBPnot_mu;
        double splitBPnot_rmu = 1.1*splitBPnot_mu;
        
        builder.setBolt("splitBP", new SplitSentenceUni(splitBP_lmu, splitBP_rmu),
                ConfigUtil.getInt(conf, "a3-splitBP.parallelism", 1)).shuffleGrouping("spout", "Bolt-P");

        builder.setBolt("splitBPnot", new SplitSentenceUni(splitBPnot_lmu, splitBPnot_rmu),
                ConfigUtil.getInt(conf, "a3-splitBPnot.parallelism", 1)).shuffleGrouping("spout", "Bolt-Pnot");
        
        ///The last bolt accept all tuples from split-BP and partial stream (Pnot) directly from spout.
        double counter_mu = ConfigUtil.getDouble(conf, "a3-counter.mu", 1.0);
        double counter_lmu = 0.9*counter_mu;
        double counter_rmu = 1.1*counter_mu;
        
        builder.setBolt("counter", new WordCounterUni(counter_lmu, counter_rmu),
        		ConfigUtil.getInt(conf, "a3-counter.parallelism", 1))
        		.shuffleGrouping("splitBP")
        		.shuffleGrouping("splitBPnot");
                
        Map<String, Object> metricsConsumerArgs = new HashMap<String, Object>();
        metricsConsumerArgs.put(RedisMetricsCollector.REDIS_HOST, host);
        metricsConsumerArgs.put(RedisMetricsCollector.REDIS_PORT, port);
        metricsConsumerArgs.put(ConsumerBase.METRICS_NAME, Arrays.asList("tuple-completed"));
        String queueName = conf.get("a3-metrics.output.queue-name").toString();
        if (queueName != null) {
            metricsConsumerArgs.put(RedisMetricsCollector.REDIS_QUEUE_NAME, queueName);
        }
        conf.registerMetricsConsumer(MetricsCollector.class, metricsConsumerArgs, 1);

        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
}
