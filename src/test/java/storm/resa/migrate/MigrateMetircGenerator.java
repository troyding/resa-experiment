package storm.resa.migrate;

import org.junit.Test;

import java.util.Random;
import java.util.stream.DoubleStream;

/**
 * Created by ding on 14-7-21.
 */
public class MigrateMetircGenerator {

    public static int[] STATES = new int[]{4, 6, 8, 10};

    @Test
    public void genMetric() {
        double[] metrics = new double[STATES.length * STATES.length];
        double sum = 0;
        Random rand = new Random();
        for (int i = 0; i < metrics.length; i++) {
            int x = i / STATES.length;
            int y = i % STATES.length;
            if (x == y) {
                metrics[i] = 0;
            } else {
                double d = Math.exp(-Math.abs(STATES[x] - STATES[y]) / 3) * Math.abs(rand.nextGaussian());
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
