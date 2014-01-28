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
public class ExpServiceWCTopology {


    public static void main(String[] args) throws Exception {

        Config conf = ConfigUtil.readConfig(new File(args[1]));

        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[1]);
        }

        TopologyBuilder builder = new TopologyBuilder();

        int numWorkers = ConfigUtil.getInt(conf, "worker.count", 1);
        int numAckers = ConfigUtil.getInt(conf, "acker.count", 1);

        conf.setNumWorkers(numWorkers);
        conf.setNumAckers(numAckers);

        String host = (String) conf.get("redis.host");
        int port = ConfigUtil.getInt(conf, "redis.port", 6379);

        if (ConfigUtil.getBoolean(conf, "spout.redis", false) == false) {
            builder.setSpout("say", new RandomSentenceSpout(), ConfigUtil.getInt(conf, "spout.parallelism", 1));
        } else {
            String queue = (String) conf.get("redis.queue");
            builder.setSpout("say", new SentenceSpout(host, port, queue),
                    ConfigUtil.getInt(conf, "spout.parallelism", 1));
        }

        ///int splitExecCount = ConfigUtil.getInt(conf, "split.parallelism", 1);
        ///int splitTaskCount = ConfigUtil.getInt(conf, "split.tasks", 1);        
        ///splitExecCount = Math.min(splitExecCount, splitTaskCount);

        ///int countExecCount = ConfigUtil.getInt(conf, "counter.parallelism", 1);
        ///int countTaskCount = ConfigUtil.getInt(conf, "counter.tasks", 1);
        ///countExecCount = Math.min(countExecCount, countTaskCount);

        ///builder.setBolt("split", new SplitSentence(), splitExecCount)//.setNumTasks(splitTaskCount)
        ///        .shuffleGrouping("say");
        ///builder.setBolt("counter", new WordCount(), countExecCount)//.setNumTasks(countTaskCount)
        ///        .fieldsGrouping("split", new Fields("word"));

        builder.setBolt("split", new SplitSentence(ConfigUtil.getDouble(conf, "split.mu", 1.0)),
                ConfigUtil.getInt(conf, "split.parallelism", 1)).shuffleGrouping("say");
        builder.setBolt("counter", new WordCounter(ConfigUtil.getDouble(conf, "counter.mu", 1.0)),
                ConfigUtil.getInt(conf, "counter.parallelism", 1)).fieldsGrouping("split", new Fields("word"));

        Map<String, Object> metricsConsumerArgs = new HashMap<String, Object>();
        metricsConsumerArgs.put(RedisMetricsCollector.REDIS_HOST, host);
        metricsConsumerArgs.put(RedisMetricsCollector.REDIS_PORT, port);
        metricsConsumerArgs.put(ConsumerBase.METRICS_NAME, Arrays.asList("tuple-completed"));
        String queueName = conf.get("metrics.output.queue-name").toString();
        if (queueName != null) {
            metricsConsumerArgs.put(RedisMetricsCollector.REDIS_QUEUE_NAME, queueName);
        }
        conf.registerMetricsConsumer(MetricsCollector.class, metricsConsumerArgs, 1);

        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
}
