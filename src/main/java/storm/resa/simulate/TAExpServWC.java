package storm.resa.simulate;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import storm.resa.measure.WinAggMeasurementCollector;
import storm.resa.measure.WinAggregateBolt;
import storm.resa.measure.WinAggregateSpout;
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
public class TAExpServWC {


    public static void main(String[] args) throws Exception {

        Config conf = ConfigUtil.readConfig(new File(args[1]));

        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[1]);
        }

        TopologyBuilder builder = new TopologyBuilder();

        int numWorkers = ConfigUtil.getInt(conf, "a1-worker.count", 1);
        int numAckers = ConfigUtil.getInt(conf, "a1-acker.count", 1);

        conf.setNumWorkers(numWorkers);
        conf.setNumAckers(numAckers);

        String host = (String) conf.get("redis.host");
        int port = ConfigUtil.getInt(conf, "redis.port", 6379);
        String queue = (String) conf.get("a1-redis.queue");

        IRichSpout spout = new WinAggregateSpout(new SentenceSpout(host, port, queue));
        builder.setSpout("sentenceSpout", spout, ConfigUtil.getInt(conf, "a1-spout.parallelism", 1));

        IRichBolt splitBolt = new WinAggregateBolt(new SplitSentence(ConfigUtil.getDouble(conf, "a1-split.mu", 1.0)));
        builder.setBolt("split", splitBolt,
                ConfigUtil.getInt(conf,  "a1-split.parallelism", 1)).shuffleGrouping("sentenceSpout");

        IRichBolt wcBolt = new WinAggregateBolt(new WordCounter(ConfigUtil.getDouble(conf, "a1-counter.mu", 1.0)));
        builder.setBolt("counter", wcBolt,
                ConfigUtil.getInt(conf,  "a1-counter.parallelism", 1)).shuffleGrouping("split");

        Map<String, Object> consumerArgs = new HashMap<>();
        consumerArgs.put(RedisMetricsCollector.REDIS_HOST, host);
        consumerArgs.put(RedisMetricsCollector.REDIS_PORT, port);
        consumerArgs.put(ConsumerBase.METRICS_NAME, Arrays.asList("tuple-completed", "__sendqueue", "__receive"));
        String queueName = (String) conf.get("a1-metrics.output.queue-name");
        if (queueName != null) {
            consumerArgs.put(RedisMetricsCollector.REDIS_QUEUE_NAME, queueName);
        }
        conf.registerMetricsConsumer(WinAggMeasurementCollector.class, consumerArgs, 1);

        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
}
