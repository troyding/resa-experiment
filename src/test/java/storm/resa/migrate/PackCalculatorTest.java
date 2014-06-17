package storm.resa.migrate;

import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.DoubleStream;

public class PackCalculatorTest {

    private PackCalculator calculator;
    private double[] dataSizes;
    private double[] workload;
    private double totalDataSize;

    @Before
    public void init() throws Exception {
        workload = Files.readAllLines(Paths.get("/Volumes/Data/work/doctor/resa/exp/workload-128.txt")).stream()
                .map(String::trim).filter(s -> !s.isEmpty()).mapToDouble(Double::valueOf).toArray();
        dataSizes = Files.readAllLines(Paths.get("/Volumes/Data/work/doctor/resa/exp/data-sizes-128.txt")).stream()
                .map(String::trim).filter(s -> !s.isEmpty()).mapToDouble(Double::valueOf).toArray();
        totalDataSize = DoubleStream.of(dataSizes).sum();
        calculator = new PackCalculator().setWorkloads(workload).setDataSizes(dataSizes).setUpperLimitRatio(1.5f);
    }

    @Test
    public void testCalc1() throws Exception {
        int currNumPacks = 4;
        int newNumPacks = 5;
        PackCalculator ret = calculator.setCurrPacks(packAvg(workload.length, currNumPacks)).setWorkloads(workload)
                .setTargetNumPacks(newNumPacks).calc();
        System.out.println(Arrays.toString(ret.getPacks()));
        System.out.println(ret.dataRemain());
        System.out.println("To move: " + (totalDataSize - ret.dataRemain()));
    }

    @Test
    public void testCalcNInc() throws Exception {
        final int currNumPacks = 4;
        int[] currPacks = packAvg(workload.length, currNumPacks);
        System.out.println("src allocation: " + Arrays.toString(currPacks));
        for (int i = currNumPacks + 2; i < 13; i += 2) {
//            System.out.println("----------------------------------------");
            PackCalculator ret = calculator.setCurrPacks(currPacks).setTargetNumPacks(i).calc();
//            System.out.println("New allocation: " + Arrays.toString(ret.getPacks()));
//            System.out.printf("Remain: %.2f KB\n", ret.dataRemain() / 1024);
//            System.out.printf("To move: %.2f KB\n", (totalDataSize - ret.dataRemain()) / 1024);
            System.out.printf("%.2f\n", (totalDataSize - ret.dataRemain()) / 1024);
            currPacks = ret.getPacks();
        }
    }

    @Test
    public void testCalcNDesc() throws Exception {
        final int currNumPacks = 12;
        int[] currPacks = packAvg(workload.length, currNumPacks);
        System.out.println("src allocation: " + Arrays.toString(currPacks));
        for (int i = currNumPacks - 1; i > 3; i -= 2) {
//            System.out.println("----------------------------------------");
            PackCalculator ret = calculator.setCurrPacks(currPacks).setTargetNumPacks(i).calc();
//            System.out.println("New allocation: " + Arrays.toString(ret.getPacks()));
//            System.out.printf("Remain: %.2f KB\n", ret.dataRemain() / 1024);
//            System.out.printf("To move: %.2f KB\n", (totalDataSize - ret.dataRemain()) / 1024);
            System.out.printf("%.2f\n", (totalDataSize - ret.dataRemain()) / 1024);
            currPacks = ret.getPacks();
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