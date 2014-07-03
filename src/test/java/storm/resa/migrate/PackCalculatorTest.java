package storm.resa.migrate;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

public class PackCalculatorTest {

    private PackCalculator calculator;
    private double[] dataSizes;
    private double[] workload;
    private double totalDataSize;

    @Before
    public void init() throws Exception {
        workload = Files.readAllLines(Paths.get("/Volumes/Data/work/doctor/resa/exp/workload-064.txt")).stream()
                .map(String::trim).filter(s -> !s.isEmpty()).mapToDouble(Double::valueOf).toArray();
        dataSizes = Files.readAllLines(Paths.get("/Volumes/Data/work/doctor/resa/exp/data-sizes-064.txt")).stream()
                .map(String::trim).filter(s -> !s.isEmpty()).mapToDouble(Double::valueOf).toArray();
        totalDataSize = DoubleStream.of(dataSizes).sum();
        calculator = new FastCalculator().setWorkloads(workload).setDataSizes(dataSizes).setUpperLimitRatio(1.3f);
    }

    @Test
    public void testOneSrcPack() throws Exception {
        int currNumPacks = 4;
        int newNumPacks = 6;
        PackCalculator ret = calculator.setSrcPack(packAvg(workload.length, currNumPacks)).setWorkloads(workload)
                .setTargetPackSize(newNumPacks).calc();
        System.out.println(Arrays.toString(packAvg(workload.length, currNumPacks)));
        Assert.assertEquals(IntStream.of(ret.getPack()).sum(), workload.length);
        System.out.println(Arrays.toString(ret.getPack()));
        System.out.println(ret.gain());
        System.out.println("To move: " + (totalDataSize - ret.gain()));
    }

    @Test
    public void testMultiSrcPack() throws Exception {
        int size = 20, targetPack = 9;
        double[] weight = new Random().doubles(size).toArray();
        double sum = DoubleStream.of(weight).sum();
        weight = DoubleStream.of(weight).map(w -> w / sum).toArray();
        Iterator<int[]> packages = IntStream.range(4, 4 + size + 1).filter(i -> i != targetPack)
                .mapToObj(i -> packAvg(workload.length, i)).iterator();
        Map<int[], Double> srcPacks = DoubleStream.of(weight).boxed()
                .collect(Collectors.toMap(w -> packages.next(), w -> w));
        PackCalculator ret = calculator.setSrcPacks(srcPacks).setTargetPackSize(targetPack).calc();
        System.out.println("Bast pack: " + Arrays.toString(ret.getPack()));
        System.out.println("Gain: " + ret.gain());
    }

    @Test
    public void testCalcNInc() throws Exception {
        final int currNumPacks = 4;
        int[] currPacks = packAvg(workload.length, currNumPacks);
        System.out.println("src allocation: " + Arrays.toString(currPacks));
        for (int i = currNumPacks + 2; i < 13; i += 2) {
//            System.out.println("----------------------------------------");
            PackCalculator ret = calculator.setSrcPack(currPacks).setTargetPackSize(i).calc();
//            System.out.println("New allocation: " + Arrays.toString(ret.getPack()));
//            System.out.printf("Remain: %.2f KB\n", ret.gain() / 1024);
//            System.out.printf("To move: %.2f KB\n", (totalDataSize - ret.gain()) / 1024);
            System.out.printf("%.2f\n", (totalDataSize - ret.gain()) / 1024);
            currPacks = ret.getPack();
        }
    }

    @Test
    public void testCalcNDesc() throws Exception {
        final int currNumPacks = 12;
        int[] currPacks = packAvg(workload.length, currNumPacks);
        System.out.println("src allocation: " + Arrays.toString(currPacks));
        for (int i = currNumPacks - 1; i > 3; i -= 2) {
//            System.out.println("----------------------------------------");
            PackCalculator ret = calculator.setSrcPack(currPacks).setTargetPackSize(i).calc();
//            System.out.println("New allocation: " + Arrays.toString(ret.getPack()));
//            System.out.printf("Remain: %.2f KB\n", ret.gain() / 1024);
//            System.out.printf("To move: %.2f KB\n", (totalDataSize - ret.gain()) / 1024);
            System.out.printf("%.2f\n", (totalDataSize - ret.gain()) / 1024);
            currPacks = ret.getPack();
        }
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