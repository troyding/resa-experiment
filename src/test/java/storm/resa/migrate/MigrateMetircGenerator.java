package storm.resa.migrate;

import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

/**
 * Created by ding on 14-7-21.
 */
public class MigrateMetircGenerator {

    public static int[] STATES = new int[]{4, 6, 8, 10};

    private int numSteps = 10000000;
    private TreeMap<String, Double> migrationMetrics;

    @Before
    public void init() throws Exception {
        migrationMetrics = Files.readAllLines(Paths.get("/Volumes/Data/work/doctor/resa/exp/transfer-p.txt")).stream()
                .map(s -> s.split(":")).collect(Collectors.toMap(strs -> strs[0] + "-" + strs[1],
                        strs -> Double.parseDouble(strs[2]), (v1, v2) -> {
                            throw new IllegalStateException();
                        }, TreeMap::new));
    }

    @Test
    public void genStateTransferMetric() {
        TreeMap<String, Integer> stat = new TreeMap<>();
        int currState = 8;
        for (int i = 1; i < numSteps; i++) {
            int nextStates = getNextState(currState);
            stat.compute(currState + "-" + nextStates, (k, v) -> v == null ? 1 : v + 1);
            currState = nextStates;
        }
        stat.forEach((k, v) -> System.out.printf("%s:%f\n", k.replace('-', ':'), (double) v / numSteps));
    }

    private int getNextState(int curr) {
        Map.Entry<String, Double>[] states = migrationMetrics.subMap(curr + "-", curr + "~").entrySet()
                .toArray(new Map.Entry[0]);
        double sum = Stream.of(states).mapToDouble(e -> e.getValue()).sum();
        double rand = Math.random();
        double d = 0;
        for (int i = 0; i < states.length; i++) {
            d += (states[i].getValue() / sum);
            if (d >= rand) {
                return Integer.parseInt(states[i].getKey().split("-")[1]);
            }
        }
        throw new IllegalStateException();
    }

    @Test
    public void genNeighStateProbability() {
        double[] metrics = new double[STATES.length * STATES.length];
        double sum = 0;
        Random rand = new Random();
        for (int i = 0; i < metrics.length; i++) {
            int x = i / STATES.length;
            int y = i % STATES.length;
            if (x == y) {
                metrics[i] = 0;
            } else {
                double d =  Math.abs(rand.nextGaussian());//Math.exp(-Math.abs(STATES[x] - STATES[y]) / 3) * Math.abs(rand.nextGaussian());
                metrics[i] = d;
                sum += d;
            }
        }
        final double finalSum = sum;
        metrics = DoubleStream.of(metrics).map(d -> d / finalSum).toArray();
        for (int i = 0; i < metrics.length; i++) {
            int x = i / STATES.length;
            int y = i % STATES.length;
            if (metrics[i] > 0.01) {
                System.out.printf("%d:%d:%.4f\n", STATES[x], STATES[y], metrics[i]);
            }
        }
    }


}
