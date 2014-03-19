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
import storm.resa.measure.MeasurableBolt;
import storm.resa.measure.MeasurableSpout;
import storm.resa.util.ConfigUtil;

import storm.resa.metric.RedisMetricsCollector;
import storm.resa.metric.ConsumerBase;
import storm.resa.simulate.MetricsCollector;

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
public class OutlierDetectionTopology {

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
        int objectCount = ConfigUtil.getIntThrow(conf, "spout.object.size");
        // use objectId+time as traceID
        IRichSpout spout = new MeasurableSpout(new ObjectSpout(host, port, queue, objectCount),
                (streamId, tuple, messageId) -> tuple.get(0) + "-" + tuple.get(2));
        builder.setSpout("objectSpout", spout, ConfigUtil.getInt(conf, "spout.parallelism", 1));

        List<double[]> randVectors = generateRandomVectors(ConfigUtil.getIntThrow(conf, "projection.dimension"),
                ConfigUtil.getIntThrow(conf, "projection.size"));
        IRichBolt projectionBolt = new MeasurableBolt(new Projection(new ArrayList<>(randVectors)),
                (tuple) -> tuple.getValueByField(ObjectSpout.ID_FILED)
                        + "-" + tuple.getValueByField(ObjectSpout.TIME_FILED));
        builder.setBolt("projection", projectionBolt,
                ConfigUtil.getInt(conf, "projection.parallelism", 1)).shuffleGrouping("objectSpout");

        int minNeighborCount = ConfigUtil.getIntThrow(conf, "detector.neighbor.count.min");
        double maxNeighborDistance = ConfigUtil.getDoubleThrow(conf, "detector.neighbor.distance.max");
        IRichBolt detectorBolt = new MeasurableBolt(new Detector(objectCount, minNeighborCount, maxNeighborDistance),
                (tuple) -> tuple.getValueByField(ObjectSpout.ID_FILED)
                        + "-" + tuple.getValueByField(ObjectSpout.TIME_FILED));
        builder.setBolt("detector", detectorBolt, ConfigUtil.getInt(conf, "detector.parallelism", 1))
                .fieldsGrouping("projection", new Fields(Projection.PROJECTION_ID_FIELD));

        IRichBolt updaterBolt = new MeasurableBolt(new Updater(randVectors.size()),
                (tuple) -> tuple.getValueByField(ObjectSpout.ID_FILED)
                        + "-" + tuple.getValueByField(ObjectSpout.TIME_FILED));
        builder.setBolt("updater", updaterBolt, ConfigUtil.getInt(conf, "updater.parallelism", 1))
                .fieldsGrouping("detector", new Fields(ObjectSpout.TIME_FILED, ObjectSpout.ID_FILED));

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