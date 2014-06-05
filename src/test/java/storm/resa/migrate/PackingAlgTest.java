package storm.resa.migrate;

import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.DoubleStream;

public class PackingAlgTest {

    @Test
    public void testCalc() throws Exception {
        double[] workload = Files.readAllLines(Paths.get("/Users/ding/Desktop/workload.txt")).stream().map(String::trim)
                .filter(s -> !s.isEmpty()).mapToDouble(Double::valueOf).toArray();
        DoubleStream.of(workload).forEach(System.out::println);
        int numPartition = 12;
        System.out.println("avg is:" + DoubleStream.of(workload).sum() / numPartition);
        int[] ret = PackingAlg.calc(workload, numPartition);
        System.out.println(Arrays.toString(ret));
        int i = 0;
        List<Long> assigment = new ArrayList<>();
        for (int j = 0; j < ret.length; j++) {
            double sum = 0;
            for (int k = 0; k < ret[j]; k++) {
                sum = sum + workload[i++];
            }
            assigment.add((long) sum);
        }
        assigment.forEach(System.out::println);
    }
}