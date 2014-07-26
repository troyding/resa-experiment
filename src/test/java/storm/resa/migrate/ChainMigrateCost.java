package storm.resa.migrate;

import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static storm.resa.migrate.PackCalculator.convertPack;

/**
 * Created by ding on 14-7-24.
 */
public class ChainMigrateCost {

    private double[] dataSizes;
    private double[] workload;
    private double totalDataSize;
    private float ratio = 1.7f;
    private int[] migrationChain;

    @Before
    public void init() throws Exception {
        workload = Files.readAllLines(Paths.get("/Volumes/Data/work/doctor/resa/exp/workload-064.txt")).stream()
                .map(String::trim).filter(s -> !s.isEmpty()).mapToDouble(Double::valueOf).toArray();
        dataSizes = Files.readAllLines(Paths.get("/Volumes/Data/work/doctor/resa/exp/data-sizes-064.txt")).stream()
                .map(String::trim).filter(s -> !s.isEmpty()).mapToDouble(Double::valueOf).toArray();
        totalDataSize = DoubleStream.of(dataSizes).sum();
        migrationChain = Files.readAllLines(Paths.get("/Volumes/Data/work/doctor/resa/exp/chain.txt")).stream()
                .mapToInt(Integer::parseInt).toArray();
    }

    @Test
    public void chainOpt() {
        int[][] packs = IntStream.of(migrationChain).mapToObj(state -> packAvg(workload.length, state))
                .toArray(int[][]::new);
        PackCalculator calculator = new DPBasedCalculator().setWorkloads(workload).setDataSizes(dataSizes)
                .setUpperLimitRatio(ratio);
        double lastGain, gain = 0;
        do {
            lastGain = gain;
            gain = 0;
            for (int i = 1; i < migrationChain.length; i++) {
                Map<int[], Double> pack = new HashMap<>();
                pack.put(packs[i - 1], 0.5);
                if (i + 1 != migrationChain.length) {
                    pack.put(packs[i + 1], 0.5);
                }
                calculator.setSrcPacks(pack).setTargetPackSize(migrationChain[i]).calc();
                packs[i] = calculator.getPack();
                gain += calculator.gain();
            }
//            System.out.println(Math.abs(gain - lastGain));
        } while (Math.abs(gain - lastGain) > 10.0);
        double toMove = 0.0;
        KuhnMunkres km = new KuhnMunkres(dataSizes.length);
        for (int i = 1; i < packs.length; i++) {
            double remain = packGain(convertPack(packs[i - 1]), convertPack(packs[i]), km);
            toMove += (totalDataSize - remain);
//            System.out.printf("%.2f\n", (totalDataSize - remain) / 1024);
        }
        System.out.println(toMove);
    }

    @Test
    public void calcLocalOptimization() {
        KuhnMunkres km = new KuhnMunkres(dataSizes.length);
        int[] states = migrationChain;
        PackCalculator calculator = new DPBasedCalculator().setWorkloads(workload).setDataSizes(dataSizes)
                .setUpperLimitRatio(ratio);
        double toMove = 0.0;
        int[] srcPack = packAvg(dataSizes.length, states[0]);
        for (int i = 1; i < states.length; i++) {
            calculator.setSrcPack(srcPack).setTargetPackSize(states[i]).calc();
            int[] newPack = calculator.getPack();
            double remain = packGain(convertPack(srcPack), convertPack(newPack), km);
            toMove += (totalDataSize - remain);
//            System.out.printf("%.2f\n", (totalDataSize - remain) / 1024);
            srcPack = newPack;
        }
        System.out.println(toMove);
    }

    private double packGain(PackCalculator.Range[] pack1, PackCalculator.Range[] pack2, KuhnMunkres kmAlg) {
        double[][] weights = new double[pack1.length][pack2.length];
        for (int i = 0; i < pack1.length; i++) {
            for (int j = 0; j < pack2.length; j++) {
                weights[i][j] = overlap(pack1[i], pack2[j]);
            }
        }
        double[] maxWeight = new double[1];
        kmAlg.getMaxBipartie(weights, maxWeight);
        return maxWeight[0];
    }

    private double overlap(PackCalculator.Range r1, PackCalculator.Range r2) {
        if (r1.end < r2.start || r1.start > r2.end) {
            return 0;
        } else if (r1.start <= r2.start && r1.end >= r2.start) {
            return IntStream.rangeClosed(r2.start, Math.min(r2.end, r1.end)).mapToDouble(i -> dataSizes[i]).sum();
        } else if (r1.start >= r2.start && r1.start <= r2.end) {
            return IntStream.rangeClosed(r1.start, Math.min(r2.end, r1.end)).mapToDouble(i -> dataSizes[i]).sum();
        }
        return 0;
    }


    private static int[] packAvg(int eleCount, int packCount) {
        int[] ret = new int[packCount];
        Arrays.fill(ret, eleCount / packCount);
        int k = eleCount % packCount;
        for (int i = 0; i < k; i++) {
            ret[i]++;
        }
        return ret;
    }

}
