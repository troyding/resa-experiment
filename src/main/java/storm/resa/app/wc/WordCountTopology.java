package storm.resa.app.wc;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.resa.spout.RedisQueueSpout;
import storm.resa.util.ConfigUtil;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class WordCountTopology {

    public static class SplitSentence extends BaseBasicBolt {

        private static final long serialVersionUID = 9182719848878455933L;

        public SplitSentence() {
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String sentence = input.getString(0);
            StringTokenizer tokenizer = new StringTokenizer(sentence, "\\s+");
            while (tokenizer.hasMoreTokens()) {
                collector.emit(Arrays.asList((Object) tokenizer.nextToken()));
            }
            // Utils.sleep(1000);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class WordCount extends BaseBasicBolt {
        private static final long serialVersionUID = 4905347466083499207L;
        private Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            count++;
            counts.put(word, count);
            collector.emit(new Values(word, count));
            // Utils.sleep(1000);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    public static final class RedisSentenceSpout extends RedisQueueSpout {

        public RedisSentenceSpout(String host, int port, String queue) {
            super(host, port, queue);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));
        }
    }

    public static void main(String[] args) throws Exception {

        Config conf = ConfigUtil.readConfig(new File(args[1]));

        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[1]);
        }

        TopologyBuilder builder = new TopologyBuilder();

        if (!ConfigUtil.getBoolean(conf, "spout.redis", false)) {
            builder.setSpout("say", new RandomSentenceSpout(), ConfigUtil.getInt(conf, "spout.parallelism", 1));
        } else {
            String host = (String) conf.get("redis.host");
            int port = ((Number) conf.get("redis.port")).intValue();
            String queue = (String) conf.get("redis.queue");
            builder.setSpout("say", new RedisSentenceSpout(host, port, queue),
                    ConfigUtil.getInt(conf, "spout.parallelism", 1));
        }

        builder.setBolt("split", new SplitSentence(), ConfigUtil.getInt(conf, "split.parallelism", 1))
                .shuffleGrouping("say");
        builder.setBolt("counter", new WordCount(), ConfigUtil.getInt(conf, "counter.parallelism", 1))
                .fieldsGrouping("split", new Fields("word"));

        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
}
