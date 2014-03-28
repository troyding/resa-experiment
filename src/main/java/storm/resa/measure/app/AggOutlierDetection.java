package storm.resa.measure.app;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.resa.app.cod.Detector;
import storm.resa.app.cod.ObjectSpout;
import storm.resa.app.cod.Projection;
import storm.resa.app.cod.Updater;
import storm.resa.measure.*;
import storm.resa.metric.ConsumerBase;
import storm.resa.metric.RedisMetricsCollector;
import storm.resa.util.ConfigUtil;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

/**
 * Created by ding on 14-3-17.
 */
public class AggOutlierDetection {

    public static List<double[]> generateRandomVectors(int dimension, int vectorCount) {
        Random rand = new Random();
        return Stream.generate(() -> {
            double[] v = DoubleStream.generate(rand::nextGaussian).limit(dimension).toArray();
            double sum = Math.sqrt(Arrays.stream(v).map((d) -> d * d).sum());
            return Arrays.stream(v).map((d) -> d / sum).toArray();
        }).limit(vectorCount).collect(Collectors.toList());
    }

    public static void main(String[] args) throws Exception {
        Config conf = ConfigUtil.readConfig(new File(args[1]));
        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[1]);
        }
        TopologyBuilder builder = new TopologyBuilder();

        int numWorkers = ConfigUtil.getInt(conf, "a-worker.count", 1);
        int numAckers = ConfigUtil.getInt(conf, "a-acker.count", 1);

        conf.setNumWorkers(numWorkers);
        conf.setNumAckers(numAckers);
        conf.setDebug(false);
        int bucketSizeSec = ConfigUtil.getInt(conf, "topology.builtin.metrics.bucket.size.secs", 60);
        conf.put("topology.builtin.metrics.bucket.size.secs", bucketSizeSec);

        //set spout
        String host = (String) conf.get("redis.host");
        int port = ((Number) conf.get("redis.port")).intValue();
        String queue = (String) conf.get("redis.queue");
        int objectCount = ConfigUtil.getIntThrow(conf, "a-spout.object.size");
        // use objectId+time as traceID
        IRichSpout spout = new MeasurableSpout(new ObjectSpout(host, port, queue, objectCount));
        builder.setSpout("objectSpout", spout, ConfigUtil.getInt(conf, "a-spout.parallelism", 1));

        List<double[]> randVectors = generateRandomVectors(ConfigUtil.getIntThrow(conf, "a-projection.dimension"),
                ConfigUtil.getIntThrow(conf, "a-projection.size"));
        IRichBolt projectionBolt = new WinAggregateBolt(new Projection(new ArrayList<>(randVectors)));
        builder.setBolt("projection", projectionBolt,
                ConfigUtil.getInt(conf, "a-projection.parallelism", 1)).shuffleGrouping("objectSpout");

        int minNeighborCount = ConfigUtil.getIntThrow(conf, "a-detector.neighbor.count.min");
        double maxNeighborDistance = ConfigUtil.getDoubleThrow(conf, "a-detector.neighbor.distance.max");
        IRichBolt detectorBolt = new WinAggregateBolt(new Detector(objectCount, minNeighborCount, maxNeighborDistance));
        builder.setBolt("detector", detectorBolt, ConfigUtil.getInt(conf, "a-detector.parallelism", 1))
                .fieldsGrouping("projection", new Fields(Projection.PROJECTION_ID_FIELD));

        IRichBolt updaterBolt = new WinAggregateBolt(new Updater(randVectors.size()));
        builder.setBolt("updater", updaterBolt, ConfigUtil.getInt(conf, "a-updater.parallelism", 1))
                .fieldsGrouping("detector", new Fields(ObjectSpout.TIME_FILED, ObjectSpout.ID_FILED));

        Map<String, Object> consumerArgs = new HashMap<>();
        consumerArgs.put(RedisMetricsCollector.REDIS_HOST, host);
        consumerArgs.put(RedisMetricsCollector.REDIS_PORT, port);
        consumerArgs.put(ConsumerBase.METRICS_NAME, Arrays.asList("tuple-completed", "__sendqueue", "__receive"));
        String queueName = (String) conf.get("metrics.output.queue-name");
        if (queueName != null) {
            consumerArgs.put(RedisMetricsCollector.REDIS_QUEUE_NAME, queueName);
        }
        conf.registerMetricsConsumer(WinAggMeasurementCollector.class, consumerArgs, 1);

        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }

}
