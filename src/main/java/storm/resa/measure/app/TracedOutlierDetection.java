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
import storm.resa.measure.MeasurableSpout;
import storm.resa.measure.TracedMeasurementCollector;
import storm.resa.measure.TracedBolt;
import storm.resa.measure.TraceIdGenerator;
import storm.resa.util.ConfigUtil;

import storm.resa.metric.RedisMetricsCollector;
import storm.resa.metric.ConsumerBase;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 14-3-17.
 */
public class TracedOutlierDetection {

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

        //set spout
        String host = (String) conf.get("redis.host");
        int port = ((Number) conf.get("redis.port")).intValue();
        String queue = (String) conf.get("redis.queue");
        int objectCount = ConfigUtil.getIntThrow(conf, "a-spout.object.size");
        // use objectId+time as traceID
        IRichSpout spout = new MeasurableSpout(new ObjectSpout(host, port, queue, objectCount),
                (streamId, tuple, messageId) -> tuple.get(0) + "-" + tuple.get(2));
        builder.setSpout("objectSpout", spout, ConfigUtil.getInt(conf, "a-spout.parallelism", 1));

        int boltSampleRate = ConfigUtil.getInt(conf, "a-bolt.sample.rate", 1);
        TraceIdGenerator.OfBolt generator = (tuple) -> {
            String id = tuple.getValueByField(ObjectSpout.ID_FILED)
                    + "-" + tuple.getValueByField(ObjectSpout.TIME_FILED);
            return id.hashCode() % boltSampleRate == 0 ? id : null;
        };

        List<double[]> randVectors = generateRandomVectors(ConfigUtil.getIntThrow(conf, "a-projection.dimension"),
                ConfigUtil.getIntThrow(conf, "a-projection.size"));
        IRichBolt projectionBolt = new TracedBolt(new Projection(new ArrayList<>(randVectors)), generator);
        builder.setBolt("projection", projectionBolt,
                ConfigUtil.getInt(conf, "a-projection.parallelism", 1)).shuffleGrouping("objectSpout");

        int minNeighborCount = ConfigUtil.getIntThrow(conf, "a-detector.neighbor.count.min");
        double maxNeighborDistance = ConfigUtil.getDoubleThrow(conf, "a-detector.neighbor.distance.max");
        IRichBolt detectorBolt = new TracedBolt(new Detector(objectCount, minNeighborCount, maxNeighborDistance),
                generator);
        builder.setBolt("detector", detectorBolt, ConfigUtil.getInt(conf, "a-detector.parallelism", 1))
                .fieldsGrouping("projection", new Fields(Projection.PROJECTION_ID_FIELD));

        IRichBolt updaterBolt = new TracedBolt(new Updater(randVectors.size()), generator);
        builder.setBolt("updater", updaterBolt, ConfigUtil.getInt(conf, "a-updater.parallelism", 1))
                .fieldsGrouping("detector", new Fields(ObjectSpout.TIME_FILED, ObjectSpout.ID_FILED));

        Map<String, Object> metricsConsumerArgs = new HashMap<>();
        metricsConsumerArgs.put(RedisMetricsCollector.REDIS_HOST, host);
        metricsConsumerArgs.put(RedisMetricsCollector.REDIS_PORT, port);
        metricsConsumerArgs.put(ConsumerBase.METRICS_NAME, Arrays.asList("tuple-completed"));
        String queueName = (String) conf.get("metrics.output.queue-name");
        if (queueName != null) {
            metricsConsumerArgs.put(RedisMetricsCollector.REDIS_QUEUE_NAME, queueName);
        }
        conf.registerMetricsConsumer(TracedMeasurementCollector.class, metricsConsumerArgs, 1);

        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }

}
