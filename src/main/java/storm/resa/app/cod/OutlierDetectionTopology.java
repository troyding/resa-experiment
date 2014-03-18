package storm.resa.app.cod;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.resa.util.ConfigUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

/**
 * Created by ding on 14-3-17.
 */
public class OutlierDetectionTopology {

    public static List<double[]> generateRandomVectors(int dimension, int vectorCount) {
        Random rand = new Random();
        return Stream.generate(() -> {
            double[] v = DoubleStream.generate(() -> rand.nextGaussian()).limit(dimension).toArray();
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
        builder.setSpout("objectSpout", new ObjectSpout(host, port, queue),
                ConfigUtil.getInt(conf, "spout.parallelism", 1));

        List<double[]> randVectors = generateRandomVectors(ConfigUtil.getIntThrow(conf, "projection.dimension"),
                ConfigUtil.getIntThrow(conf, "projection.size"));
        builder.setBolt("projection", new Projection(new ArrayList<>(randVectors)),
                ConfigUtil.getInt(conf, "projection.parallelism", 1)).shuffleGrouping("objectSpout");

        int objectCount = ConfigUtil.getIntThrow(conf, "projection.dimension");
        int minNeighborCount = ConfigUtil.getIntThrow(conf, "detector.neighbor.count.min");
        double maxNeighborDistance = ConfigUtil.getDoubleThrow(conf, "detector.neighbor.count.min");
        builder.setBolt("detector", new Detector(objectCount, minNeighborCount, maxNeighborDistance),
                ConfigUtil.getInt(conf, "detector.parallelism", 1))
                .fieldsGrouping("projection", new Fields(Projection.PROJECTION_ID_FIELD));
        builder.setBolt("updater", new Updater(randVectors.size()), ConfigUtil.getInt(conf, "updater.parallelism", 1))
                .fieldsGrouping("detector", new Fields(ObjectSpout.TIME_FILED, ObjectSpout.ID_FILED));

        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }

}
