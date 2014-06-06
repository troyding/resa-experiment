package storm.resa.migrate;

import backtype.storm.scheduler.ExecutorDetails;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Stream;

public class KuhnMunkresTest {

    @Test
    public void testTaskOverlap() throws Exception {
        Assert.assertEquals(0, overlap(new ExecutorDetails(1, 3), new ExecutorDetails(4, 6)));
        Assert.assertEquals(2, overlap(new ExecutorDetails(1, 5), new ExecutorDetails(4, 6)));
        Assert.assertEquals(1, overlap(new ExecutorDetails(1, 4), new ExecutorDetails(4, 6)));
        Assert.assertEquals(3, overlap(new ExecutorDetails(2, 4), new ExecutorDetails(1, 6)));
        Assert.assertEquals(3, overlap(new ExecutorDetails(1, 6), new ExecutorDetails(3, 5)));
    }

    @Test
    public void testGetMaxBipartie() throws Exception {
        double[] workload = Files.readAllLines(Paths.get("/Users/ding/Desktop/workload.txt")).stream().map(String::trim)
                .filter(s -> !s.isEmpty()).mapToDouble(Double::valueOf).toArray();
        ExecutorDetails[] ret1 = allocation2Range(PackingAlg.calc(workload, 3));
        ExecutorDetails[] ret2 = allocation2Range(PackingAlg.calc(workload, 4));
        System.out.println(Arrays.toString(ret1));
        System.out.println(Arrays.toString(ret2));
        double[][] weight = new double[ret1.length][ret2.length];
        for (int i = 0; i < ret1.length; i++) {
            for (int j = 0; j < ret2.length; j++) {
                weight[i][j] = overlap(ret1[i], ret2[j]);
            }
        }
        KuhnMunkres km = new KuhnMunkres(Math.max(ret1.length, ret2.length));
        int[][] res = km.getMaxBipartie(weight, new double[1]);
        Stream.of(res).forEach(e -> System.out.println(Arrays.toString(e)));
    }

    private ExecutorDetails[] allocation2Range(int[] allc) {
        ExecutorDetails[] ret = new ExecutorDetails[allc.length];
        int k = 0;
        for (int i = 0; i < allc.length; i++) {
            int next = k + allc[i];
            ret[i] = new ExecutorDetails(k, next - 1);
            k = next;
        }
        return ret;
    }

    private int overlap(ExecutorDetails e1, ExecutorDetails e2) {
        if (e1.getStartTask() <= e2.getStartTask() && e1.getEndTask() >= e2.getStartTask()) {
            return Math.min(e2.getEndTask(), e1.getEndTask()) - e2.getStartTask() + 1;
        } else if (e1.getStartTask() >= e2.getStartTask() && e1.getStartTask() <= e2.getEndTask()) {
            return Math.min(e1.getEndTask(), e2.getEndTask()) - e1.getStartTask() + 1;
        }
        return 0;
    }

}