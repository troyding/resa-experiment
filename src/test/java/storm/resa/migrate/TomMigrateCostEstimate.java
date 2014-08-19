package storm.resa.migrate;

import clojure.lang.MapEntry;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static storm.resa.migrate.PackCalculator.Range;
import static storm.resa.migrate.PackCalculator.convertPack;

/**
 * Created by ding on 14-7-21.
 */
public class TomMigrateCostEstimate {

    private double[] dataSizes;
    private double[] workload;
    private double totalDataSize;
    private TreeMap<String, Double> migrationMetrics;
    private float ratio = 1.3f;

    private int bufferSize = 1024 * 1024;

    private String workingPath = "C:\\Users\\Tom.fu\\Desktop\\storm-experiment\\expData\\";

    @Before
    public void init() throws Exception {
        workload = Files.readAllLines(Paths.get(workingPath + "workload-032.txt")).stream()
                .map(String::trim).filter(s -> !s.isEmpty()).mapToDouble(Double::valueOf).toArray();
        dataSizes = Files.readAllLines(Paths.get(workingPath + "data-sizes-032.txt")).stream()
                .map(String::trim).filter(s -> !s.isEmpty()).mapToDouble(Double::valueOf).toArray();
        totalDataSize = DoubleStream.of(dataSizes).sum();
        migrationMetrics = Files.readAllLines(Paths.get(workingPath + "metrics.txt")).stream()
                .map(s -> s.split(":")).collect(Collectors.toMap(strs -> strs[0] + "-" + strs[1],
                        strs -> Double.parseDouble(strs[2]), (v1, v2) -> {
                            throw new IllegalStateException();
                        }, TreeMap::new));
    }

    /**
     * generate next state according to the transform matrix
     *
     * @param curr
     * @return
     */
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

    /**
     * Generate transformation probability of next states, given current state.
     *
     * @param curr
     * @return
     */
    private Map<Integer, Double> getTargetState(int curr) {
        Map<Integer, Double> states = migrationMetrics.subMap(curr + "-", curr + "~").entrySet().stream()
                .collect(Collectors.toMap(e -> Integer.parseInt(e.getKey().split("-")[1]), e -> e.getValue()));
        double sum = states.values().stream().mapToDouble(d -> d).sum();
        for (Map.Entry<Integer, Double> entry : states.entrySet()) {
            double newValue = entry.getValue() / sum;
            entry.setValue(newValue);
        }
        return states;
    }

    private Map<Integer, Double> getNeighState(int curr) {
        Map<Integer, Double> states = new HashMap<>();
        String currState = String.valueOf(curr);
        migrationMetrics.entrySet().stream().filter(e -> e.getKey().contains(currState)).forEach(e -> {
            String[] stateStrs = e.getKey().split("-");
            String nei = stateStrs[0].equals(currState) ? stateStrs[1] : stateStrs[0];
            states.compute(Integer.valueOf(nei), (k, v1) -> v1 == null ? e.getValue() : v1 + e.getValue());
        });
        double sum = states.values().stream().mapToDouble(d -> d).sum();
        for (Map.Entry<Integer, Double> entry : states.entrySet()) {
            double newValue = entry.getValue() / sum;
            entry.setValue(newValue);
        }
        return states;
    }


    @Test
    public void compare() {
        int[] states = new int[200];
        states[0] = 8;
        for (int i = 1; i < states.length; i++) {
            states[i] = getNextState(states[i - 1]);
        }
        System.out.println(Arrays.toString(states));
        for (int i = 1; i < states.length; i++) {
            System.out.print(getTargetState(states[i - 1]).get(states[i]));
            System.out.print(",");
        }
        System.out.println();
        KuhnMunkres km = new KuhnMunkres(dataSizes.length);
        System.out.println("local opt: " + calcLocalOptimization(states, km));
        System.out.println("global opt1: " + calcGlobalOptimization1(states, km));
    }

    /**
     * consider the current state and partition, and future state transition probability matrix
     * make a one time plan but considering the future gain.
     *
     * @param states
     * @param km
     * @return
     */
    private double calcGlobalOptimization1111(int[] states, KuhnMunkres km) {
        Map<Integer, int[]> state2Pack = IntStream.of(MigrateMetircGenerator.STATES).boxed()
                .collect(Collectors.toMap(i -> i, i -> PackingAlg.calc(workload, i)));
        double toMove = 0.0;
        //System.out.println(Arrays.toString(state2Pack.get(states[0])));
        for (int i = 1; i < states.length; i++) {
            int[] last = state2Pack.get(states[i - 1]);
            calcBest1(states[i - 1], states[i], state2Pack);
            double remain = packGain(convertPack(last), convertPack(state2Pack.get(states[i])), km);
//            System.out.printf("%.2f\n", (totalDataSize - remain) / 1024);
            toMove += (totalDataSize - remain);
            //System.out.println(totalDataSize - remain + ", " + toMove);
        }
        return toMove;
    }

    private double calcGlobalOptimization1(int[] states, KuhnMunkres km) {
        Map<Integer, int[]> state2Pack = IntStream.of(MigrateMetircGenerator.STATES).boxed()
                .collect(Collectors.toMap(i -> i, i -> PackingAlg.calc(workload, i)));
        double toMove = 0.0;
        System.out.println(Arrays.toString(state2Pack.get(states[0])));
        for (int i = 1; i < states.length; i++) {
            Range[] currPack = convertPack(state2Pack.get(states[i - 1]));
            calcBest1(states[i - 1], states[i], state2Pack);
            double remain = packGain(currPack, convertPack(state2Pack.get(states[i])), km);
//            System.out.printf("%.2f\n", (totalDataSize - remain) / 1024);
            toMove += (totalDataSize - remain);
            //System.out.println(totalDataSize - remain + ", " + toMove);
        }
        return toMove;
    }

    private void calcBest1(int currState, int nextStat, Map<Integer, int[]> state2Pack) {
        int[] states = MigrateMetircGenerator.STATES;
        Map<Integer, Double> gain = new TreeMap<>();
        PackCalculator calculator = new DPBasedCalculator().setWorkloads(workload).setDataSizes(dataSizes)
                .setUpperLimitRatio(ratio);
        //  Map<Integer, Double> newGain = new HashMap<>();
        int[] initState = Arrays.copyOf(state2Pack.get(currState), currState);
        int j;
        do {
            j = 0;
            for (int i = 0; i < states.length; i++) {
                Map<int[], Double> packs = new HashMap<>();
                getTargetState(states[i]).forEach((state, p) -> packs.put(state2Pack.get(state), p));
                if (states[i] == nextStat) {
//                    for (Map.Entry<int[], Double> entry : packs.entrySet()) {
//                        entry.setValue(entry.getValue() * 0.5);
//                    }
                    packs.put(initState, 4.0);
                }
                calculator.setSrcPacks(packs).setTargetPackSize(states[i]).calc();
                state2Pack.put(states[i], calculator.getPack());
                double g = calculator.gain();
                Double oldGain = gain.put(states[i], g);
                if (oldGain == null || Math.abs(g - oldGain) > 100) {
                    j++;
                }
            }
//            System.out.println(gain);
        } while (j > 0);
//        System.out.println("-----------");
    }


    /**
     * Local (greedy) algorithm, given the current state partition, and next state (number of partitions)
     * to find the migration plan which minimizes the migration cost, only consider one time migration
     *
     * @param states
     * @param km
     * @return
     */
    private double calcLocalOptimization(int[] states, KuhnMunkres km) {
        PackCalculator calculator = new DPBasedCalculator().setWorkloads(workload).setDataSizes(dataSizes)
                .setUpperLimitRatio(ratio);
        double toMove = 0.0;
        ///int[] srcPack = packAvg(dataSizes.length, states[0]);
        int[] srcPack = PackingAlg.calc(workload, states[0]);
        //System.out.println(Arrays.toString(srcPack));
        for (int i = 1; i < states.length; i++) {
            calculator.setSrcPack(srcPack).setTargetPackSize(states[i]).calc();
            int[] newPack = calculator.getPack();
            //System.out.println(Arrays.toString(newPack));
            double remain = packGain(convertPack(srcPack), convertPack(newPack), km);
            toMove += (totalDataSize - remain);
//            System.out.printf("%.2f\n", (totalDataSize - remain) / 1024);
            srcPack = newPack;
        }
        return toMove;
    }

    private double packGain(Range[] pack1, Range[] pack2, KuhnMunkres kmAlg) {
        double[][] weights = new double[pack1.length][pack2.length];
        for (int i = 0; i < pack1.length; i++) {
            for (int j = 0; j < pack2.length; j++) {
                weights[i][j] = overlap(pack1[i], pack2[j]);
            }
        }
        //System.out.println(Arrays.toString(pack1) + " " +Arrays.toString(pack2) );
        double[] maxWeight = new double[1];
        kmAlg.getMaxBipartie(weights, maxWeight);
        return maxWeight[0];
    }

    private double overlap(Range r1, Range r2) {
        if (r1.end < r2.start || r1.start > r2.end) {
            return 0;
        } else if (r1.start <= r2.start && r1.end >= r2.start) {
            return IntStream.rangeClosed(r2.start, Math.min(r2.end, r1.end)).mapToDouble(i -> dataSizes[i]).sum();
        } else if (r1.start >= r2.start && r1.start <= r2.end) {
            return IntStream.rangeClosed(r1.start, Math.min(r2.end, r1.end)).mapToDouble(i -> dataSizes[i]).sum();
        }
        return 0;
    }


    /**
     * This results in a close to "average" partition.
     * int[] srcPack = packAvg(dataSizes.length, states[0]);
     *
     * @param eleCount:  number of tasks
     * @param packCount: number of partitions
     * @return
     */
    private static int[] packAvg(int eleCount, int packCount) {
        int[] ret = new int[packCount];
        Arrays.fill(ret, eleCount / packCount);
        int k = eleCount % packCount;
        for (int i = 0; i < k; i++) {
            ret[i]++;
        }
        return ret;
    }


    /*here starts the code by Tom*/
    protected static class FeasiblePartitionPlan {
        public final List<int[]> feasiblePartitionPlan;
        public final int numberOfPartitions;

        FeasiblePartitionPlan(int numberOfPartitions, List<int[]> feasiblePartitionPlan) {
            if (feasiblePartitionPlan.size() > 0 && feasiblePartitionPlan.get(0).length != numberOfPartitions) {
                throw new IllegalArgumentException(
                        "feasiblePartitionPlan.get(0).length != numberOfPartitions");
            }
            this.numberOfPartitions = numberOfPartitions;
            this.feasiblePartitionPlan = feasiblePartitionPlan;
        }
    }

    @Test
    public void compareTest() {

        //int[] states = new int[100];//8, 10, 4, 6, 8
        int[] states = new int[]{8, 6, 4, 7, 6, 7, 6, 8, 6, 4, 8, 7, 4, 8, 7, 6, 4, 8, 6, 4, 7, 4, 8, 7, 6, 4, 8, 7, 6, 4, 8, 6, 7, 6, 8, 7, 6, 5, 8, 7, 6, 4, 8, 7, 6, 7, 4, 8, 7, 4};
        //int[] states = new int[]{8, 6, 4, 7, 6, 7, 6, 8, 6, 4, 8, 7, 4, 8, 7, 6, 4, 8, 6, 4, 7, 4, 8, 7, 6, 4, 8, 7, 6, 4, 8, 6, 7, 6, 8, 7, 6, 5, 8, 7, 6, 4, 8, 7, 6, 7, 4, 8, 7, 4, 8, 6, 5, 6,
        //4, 8, 6, 5, 6, 5, 8, 6, 5, 6, 5, 4, 7, 6, 4, 8, 7, 6, 8, 7, 4, 8, 6, 7, 6, 7, 4, 8, 6, 5, 4, 7, 4, 8, 7, 6, 8, 7, 6, 5, 4, 7, 6, 4, 8, 6};

        //int[] states = new int[]{8, 7, 6, 7, 4, 8, 6, 5, 4, 8};
        states[0] = 8;
        for (int i = 1; i < states.length; i++) {
            //states[i] = getNextState(states[i - 1]);
        }

        System.out.println(Arrays.toString(states));
        for (int i = 1; i < states.length; i++) {
            System.out.print(getTargetState(states[i - 1]).get(states[i]));
            System.out.print(",");
        }
        System.out.println();
        KuhnMunkres km = new KuhnMunkres(dataSizes.length);
        long start = System.currentTimeMillis();
        System.out.println("local opt: " + calcLocalOptimization(states, km));
        long end = System.currentTimeMillis();
        System.out.println("local opt elapse: " + (end - start));
        start = end;
        System.out.println("global opt1: " + calcGlobalOptimization1(states, km));
        end = System.currentTimeMillis();
        System.out.println("global opt1 elapse: " + (end - start));

        start = end;
        String tempFolder = workingPath + "tempData";
        System.out.println("calcOptimalChainFile: " + calcOptimalChainWithFile(states, km, tempFolder, false));
        end = System.currentTimeMillis();
        System.out.println("calcOptimalChainFile time elapse: " + (end - start));

        start = end;
        calcOptimalMatrixWithFileTraining(MigrateMetircGenerator.STATES, km, tempFolder, false, 0.1f, 2000, 0.9f);
        end = System.currentTimeMillis();
        System.out.println("calcOptimalMatrixWithFileTraining time elapse: " + (end - start));

        start = end;
        System.out.println("calcOptimalMatrixWithFileNoDecisionFile: " + calcOptimalMatrixWithFileNoDecisionFile(states, km, tempFolder, 0.9f));
        end = System.currentTimeMillis();
        System.out.println("calcOptimalMatrixWithFileNoDecisionFile time elapse: " + (end - start));

    }

    /**
     * Fast but memory cost
     *
     * @param states
     * @param km
     * @return
     */
    private double calcOptimalChain(int[] states, KuhnMunkres km) {
        int maxStateValue = states[0];
        for (int i = 1; i < states.length; i++) {
            if (states[i] > maxStateValue) {
                maxStateValue = states[i];
            }
        }

        Map<String, Float> migCost = new HashMap<>();
        //FeasiblePartitionPlan[1] - FeasiblePartitionPlan[maxStateValue], FeasiblePartitionPlan[0] is wasted
        FeasiblePartitionPlan[] feasiblePartitionPlanArray = new FeasiblePartitionPlan[maxStateValue + 1];
        for (int i = 0; i < feasiblePartitionPlanArray.length; i++) {
            feasiblePartitionPlanArray[i] = null;
        }
        float[] currPartitionValues = null;
        float[] nextPartitionValues = null;
        for (int i = states.length - 1; i >= 0; i--) {
            int totalPartitionCountCurr = states[i];

            if (feasiblePartitionPlanArray[totalPartitionCountCurr] == null) {
                double loadUpperLimit = getLoadUpperLimit(totalPartitionCountCurr);
                double[] normalizedWordloads = new double[workload.length];
                for (int j = 0; j < normalizedWordloads.length; j++) {
                    if (workload[j] > loadUpperLimit) {
                        normalizedWordloads[j] = loadUpperLimit;
                    } else {
                        normalizedWordloads[j] = workload[j];
                    }
                }
                List<int[]> feasiblePartitionPlan = new ArrayList<>();
                split(0, new int[0], totalPartitionCountCurr, totalPartitionCountCurr,
                        loadUpperLimit, normalizedWordloads, feasiblePartitionPlan);
                feasiblePartitionPlanArray[totalPartitionCountCurr] =
                        new FeasiblePartitionPlan(totalPartitionCountCurr, feasiblePartitionPlan);
            }

            FeasiblePartitionPlan currPlanList = feasiblePartitionPlanArray[totalPartitionCountCurr];
            currPartitionValues = new float[currPlanList.feasiblePartitionPlan.size()];

            if (i == states.length - 1) {
                ///last state
                for (int j = 0; j < currPartitionValues.length; j++) {
                    currPartitionValues[j] = 0.0f;
                }
            } else {
                //for (int j =0; j < currPartitionValues.length; j ++) {
                //    currPartitionValues[j] = Double.MAX_VALUE;
                //}

                int totalPartitionCountNext = states[i + 1];//next state partition
                FeasiblePartitionPlan nextPlanList = feasiblePartitionPlanArray[totalPartitionCountNext];
                if (nextPartitionValues.length != nextPlanList.feasiblePartitionPlan.size()) {
                    throw new IllegalArgumentException(
                            "nextPartitionValues.length != nextPlanList.feasiblePartitionPlan.size()");
                }

                for (int j = 0; j < currPartitionValues.length; j++) {
                    float minValue = Float.MAX_VALUE;
                    int[] srcPartition = currPlanList.feasiblePartitionPlan.get(j);

                    for (int k = 0; k < nextPartitionValues.length; k++) {
                        int[] dstPartition = nextPlanList.feasiblePartitionPlan.get(k);
                        String keyStr = Arrays.toString(new int[]{totalPartitionCountCurr, j, totalPartitionCountNext, k});

                        float remain = migCost.computeIfAbsent(keyStr, kk -> (float) packGain(convertPack(srcPartition), convertPack(dstPartition), km));
                        ///double remain = packGain(convertPack(srcPartition), convertPack(dstPartition), km);
                        float cost = (float) totalDataSize - remain + nextPartitionValues[k];
                        if (cost < minValue) {
                            minValue = cost;
                        }
                    }
                    currPartitionValues[j] = minValue;
                }
            }
            nextPartitionValues = currPartitionValues;
            currPartitionValues = null;
            if (i % 10 == 0) {
                System.out.println("Finish: " + i);
            }
        }
        ///finish iteration
        int[] init = PackingAlg.calc(workload, states[0]);
        int[] minPartition = null;
        int totalPartitionCountCurr = states[0];
        FeasiblePartitionPlan currPlanList = feasiblePartitionPlanArray[totalPartitionCountCurr];
        float tmpMin = Float.MAX_VALUE;
        float targetCost = Float.MAX_VALUE;
        if (currPlanList.feasiblePartitionPlan.size() != nextPartitionValues.length) {
            throw new IllegalArgumentException(
                    "currPlanList.feasiblePartitionPlan.size() != nextPartitionValues.length");
        }
        for (int i = 0; i < nextPartitionValues.length; i++) {
            if (nextPartitionValues[i] < tmpMin) {
                tmpMin = nextPartitionValues[i];
                minPartition = currPlanList.feasiblePartitionPlan.get(i);
            }
            if (Arrays.equals(currPlanList.feasiblePartitionPlan.get(i), init)) {
                targetCost = nextPartitionValues[i];
                System.out.println(Arrays.toString(init) + "," + Arrays.toString(currPlanList.feasiblePartitionPlan.get(i)));
            }
            //System.out.println(Arrays.toString(currPlanList.feasiblePartitionPlan.get(i)));
        }
        System.out.println("global opt2, min: " + Arrays.toString(minPartition) + ", minCost: " + tmpMin);
        return targetCost;
    }

    /**
     * A slower version, but save memory,
     * There can be another version that takes even less memory, but much slower (efficient when states is small (< 20))
     * Modification on Aug 15, remove value array (out of range), to process it in the file style
     *
     * @param states
     * @param km
     * @param tempFolder
     * @param clearFile
     * @return
     */
    private float calcOptimalChainWithFile(int[] states, KuhnMunkres km, String tempFolder, boolean clearFile) {
        //first delete temp files
        File folder = new File(tempFolder);
        File[] listOfFiles = folder.listFiles();
        if (clearFile) {
            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile()) {
                    listOfFiles[i].delete();
                }
            }
        }

        int maxStateValue = states[0];
        for (int i = 1; i < states.length; i++) {
            if (states[i] > maxStateValue) {
                maxStateValue = states[i];
            }
        }
        //this array stores the number of possible partitions for each different partitions: |S_i| for all i
        int[] planSize = new int[maxStateValue + 1];
        for (int i = 0; i < planSize.length; i++) {
            planSize[i] = 0;
        }

        int[] init = PackingAlg.calc(workload, states[0]);
        float targetCost = Float.MAX_VALUE;
        float overallMinCost = Float.MAX_VALUE;
        int[] overallMinPartition = null;

        //these two are values for each partition after each step (from end to start)
        float[] currPartitionValues = null;
        float[] nextPartitionValues = null;
        for (int i = states.length - 1; i >= 0; i--) {
            int totalPartitionCountCurr = states[i];

            //check when the possible feasible partition at current state i is derived before (saved in a temp file)
            //if not, derive it (time consuming, consider all possibilities)
            //therefore, only try to deriving once, later re-use by saving it to a temp file
            String fileCurr = tempFolder + "\\State-" + totalPartitionCountCurr + ".txt";
            if (!Files.exists(Paths.get(fileCurr))) {
                try {
                    BufferedWriter bw = Files.newBufferedWriter(Paths.get(fileCurr), StandardOpenOption.CREATE_NEW);
                    double loadUpperLimit = getLoadUpperLimit(totalPartitionCountCurr);
                    double[] normalizedWordloads = new double[workload.length];
                    for (int j = 0; j < normalizedWordloads.length; j++) {
                        if (workload[j] > loadUpperLimit) {
                            normalizedWordloads[j] = loadUpperLimit;
                        } else {
                            normalizedWordloads[j] = workload[j];
                        }
                    }
                    split(0, new int[0], totalPartitionCountCurr, totalPartitionCountCurr,
                            loadUpperLimit, normalizedWordloads, bw);
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            //save the count |S_i|
            if (planSize[totalPartitionCountCurr] == 0) {
                planSize[totalPartitionCountCurr] = getCount(fileCurr);
            }

            currPartitionValues = new float[planSize[totalPartitionCountCurr]];

            if (i == states.length - 1) {
                ///last state, set all the values to 0
                for (int j = 0; j < currPartitionValues.length; j++) {
                    currPartitionValues[j] = 0.0f;
                }
            } else {
                int totalPartitionCountNext = states[i + 1];//next state partition
                String fileNext = tempFolder + "\\State-" + totalPartitionCountNext + ".txt";
                if (planSize[totalPartitionCountNext] == 0 || !Files.exists(Paths.get(fileNext))) {
                    throw new IllegalArgumentException(
                            "planSize[totalPartitionCountNext] == 0, totalPartitionCountNext: " + totalPartitionCountNext);
                }

                ///to see the migration cost is derived before or not
                //since this is also time consuming, try to save the results for future use
                String fileCost = tempFolder + "\\Cost-" + totalPartitionCountCurr + "-" + totalPartitionCountNext + ".txt";
                boolean costMapExist = false;
                if (Files.exists(Paths.get(fileCost))) {
                    costMapExist = true;
                }
                DataInputStream brCost = null;
                DataOutputStream bwCost = null;
                String rdLineCost = null;

                String fileDec = tempFolder + "\\Decision\\Step-" + i + ".txt";
                try {
                    Files.deleteIfExists(Paths.get(fileDec));
                } catch (IOException e) {
                    e.printStackTrace();
                }

                //now, starting the two layer iteration, this cause |S_n| * |S_(n+1)| number of iterations.
                try {
                    if (costMapExist) {
                        brCost = new DataInputStream(new BufferedInputStream(Files.newInputStream(Paths.get(fileCost)), bufferSize));
                                //Files.newBufferedReader(Paths.get(fileCost));
                    } else {
                        bwCost = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(Paths.get(fileCost)), bufferSize));
                        //bwCost = Files.newBufferedWriter(Paths.get(fileCost));
                    }
                    BufferedWriter bwDec = Files.newBufferedWriter(Paths.get(fileDec));
                    BufferedReader brCurr = Files.newBufferedReader(Paths.get(fileCurr));
                    String rdLineCurr = null;
                    int currIndex = 0;
                    while ((rdLineCurr = brCurr.readLine()) != null) {
                        float minValue = Float.MAX_VALUE;
                        int optimalDicision = Integer.MAX_VALUE;
                        String[] itemsCurr = rdLineCurr.replaceAll("\\[", "").replaceAll("\\]", "").split(",");
                        int[] srcPartition = new int[itemsCurr.length];
                        for (int j = 0; j < itemsCurr.length; j++) {
                            srcPartition[j] = Integer.parseInt(itemsCurr[j].trim());
                        }
                        BufferedReader brNext = Files.newBufferedReader(Paths.get(fileNext));
                        String rdLineNext = null;
                        int nextIndex = 0;
                        while ((rdLineNext = brNext.readLine()) != null) {
                            String[] itemsNext = rdLineNext.replaceAll("\\[", "").replaceAll("\\]", "").split(",");
                            int[] dstPartition = new int[itemsNext.length];
                            for (int j = 0; j < itemsNext.length; j++) {
                                dstPartition[j] = Integer.parseInt(itemsNext[j].trim());
                            }

                            float remain = 0.0f;
                            //int offset = currIndex * planSize[totalPartitionCountNext] + nextIndex;
                            if (costMapExist) {
                                //rdLineCost = brCost.readLine();
                                //remain = Float.parseFloat(rdLineCost.trim());
                                remain = brCost.readFloat();
                            } else {
                                remain = (float) packGain(convertPack(srcPartition), convertPack(dstPartition), km);
                                //bwCost.write(Float.toString(remain));
                                //bwCost.newLine();
                                bwCost.writeFloat(remain);
                            }
                            float cost = (float) totalDataSize - remain + nextPartitionValues[nextIndex];
                            if (cost < minValue) {
                                minValue = cost;
                                optimalDicision = nextIndex;
                            }
                            nextIndex++;
                        }
                        brNext.close();
                        //for each s \in S_n, save the minimum cost
                        currPartitionValues[currIndex] = minValue;
                        bwDec.write(String.valueOf(optimalDicision));
                        bwDec.newLine();
                        currIndex++;
                        //if back to the first state, output results
                        if (i == 0) {
                            if (Arrays.equals(srcPartition, init)) {
                                targetCost = minValue;
                                System.out.println(Arrays.toString(init) + "," + Arrays.toString(srcPartition));
                            }
                            if (minValue < overallMinCost) {
                                overallMinCost = minValue;
                                overallMinPartition = srcPartition;
                            }
                        }
                    }
                    brCurr.close();
                    bwDec.close();
                    //save the computation of each d(s1, s2)
                    if (costMapExist) {
                        brCost.close();
                    } else {
                        bwCost.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            nextPartitionValues = currPartitionValues;
            currPartitionValues = null;
            if (i % 10 == 0) {
                System.out.println("Finish: " + i);
            }
        }
        ///finish iteration
        System.out.println("global opt2, min: " + Arrays.toString(overallMinPartition) + ", minCost: " + overallMinCost);
        return targetCost;
    }

    /**
     * Calculate best decision given the transportation matrix, instead of a clear chain
     *
     * @param states,     only used to calculate the migration cost.
     * @param km
     * @param tempFolder, if calcOptimalChainWithFile is called before, the results can be directly re-used.
     * @param clearFile,  by default should be false
     * @return
     */
    private void calcOptimalMatrixWithFileTraining(int[] states, KuhnMunkres km, String tempFolder, boolean clearFile,
                                                   float epsilon, int maxIteration, float gamma) {
        //first delete temp files
        clearTmpFiles(tempFolder, clearFile);

        List<Integer> stateValueSet = new ArrayList<>();
        int maxStateValue = states[0];
        stateValueSet.add(states[0]);
        for (int i = 1; i < states.length; i++) {
            if (states[i] > maxStateValue) {
                maxStateValue = states[i];
            }
            if (!stateValueSet.contains(states[i])) {
                stateValueSet.add(states[i]);
            }
        }
        //initialization
        //this array stores the number of possible partitions for each different partitions: |S_i| for all i
        //We also need to create temp files initially.
        int[] planSize = new int[maxStateValue + 1];
        for (int i = 0; i < planSize.length; i++) {
            planSize[i] = 0;
        }

        for (int i = 0; i < stateValueSet.size(); i++) {
            int totalPartitionCountCurr = stateValueSet.get(i);
            System.out.println("Init state: " + totalPartitionCountCurr);
            String fileCurr = tempFolder + "\\State-" + totalPartitionCountCurr + ".txt";
            if (!Files.exists(Paths.get(fileCurr))) {
                try {
                    BufferedWriter bw = Files.newBufferedWriter(Paths.get(fileCurr), StandardOpenOption.CREATE_NEW);
                    double loadUpperLimit = getLoadUpperLimit(totalPartitionCountCurr);
                    double[] normalizedWordloads = new double[workload.length];
                    for (int j = 0; j < normalizedWordloads.length; j++) {
                        if (workload[j] > loadUpperLimit) {
                            normalizedWordloads[j] = loadUpperLimit;
                        } else {
                            normalizedWordloads[j] = workload[j];
                        }
                    }
                    split(0, new int[0], totalPartitionCountCurr, totalPartitionCountCurr,
                            loadUpperLimit, normalizedWordloads, bw);
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("File not exist: " + fileCurr + ", finish create");
            }

            planSize[totalPartitionCountCurr] = getCount(fileCurr);
            String fileCurrValue = tempFolder + "\\values\\Value-" + totalPartitionCountCurr + ".txt";
            if (!Files.exists(Paths.get(fileCurrValue))) {
                try {
                    DataOutputStream bwCost = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(Paths.get(fileCurrValue)), bufferSize));
                    for (int j = 0; j < planSize[totalPartitionCountCurr]; j++) {
                        bwCost.writeFloat(0.0f);
                    }
                    bwCost.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("Initialize: " + fileCurrValue + ", cnt: " + planSize[totalPartitionCountCurr]);
            } else {
                System.out.println("Find existing file: " + fileCurrValue);
            }
        }

        int iterates = 0;
        float diff = 0.0f;
        //starting iteration
        while (iterates <= maxIteration) {
            diff = 0.0f;
            for (int i = 0; i < stateValueSet.size(); i++) {
                //go through all states
                int totalPartitionCountCurr = stateValueSet.get(i);
                Map<Integer, Double> possibleState = getTargetState(totalPartitionCountCurr);

                //initialize the values in this new round
                float[] newRoundPartitionValues = new float[planSize[totalPartitionCountCurr]];
                for (int j = 0; j < newRoundPartitionValues.length; j++) {
                    newRoundPartitionValues[j] = 0.0f;
                }

                String fileCurr = tempFolder + "\\State-" + totalPartitionCountCurr + ".txt";
                if (!Files.exists(Paths.get(fileCurr))) {
                    throw new IllegalArgumentException(
                            "!Files.exists(Paths.get(fileCurr): " + fileCurr);
                }

                //go through each next state neighbor state of current state
                for (Map.Entry<Integer, Double> entry : possibleState.entrySet()) {
                    int totalPartitionCountNext = entry.getKey();
                    double prob = entry.getValue();

                    ///in this algorithm, we also need to record the path of optimial decision for each next neighbor
                    int[] currDecisionForNextPartition = new int[planSize[totalPartitionCountCurr]];

                    ///first get state partitions of next state
                    String fileNext = tempFolder + "\\State-" + totalPartitionCountNext + ".txt";
                    if (planSize[totalPartitionCountNext] == 0 || !Files.exists(Paths.get(fileNext))) {
                        throw new IllegalArgumentException(
                                "planSize[totalPartitionCountNext] == 0, totalPartitionCountNext: " + totalPartitionCountNext);
                    }

                    ///then get the mig cost caches if exist
                    String fileCost = tempFolder + "\\Cost-" + totalPartitionCountCurr + "-" + totalPartitionCountNext + ".txt";
                    boolean costMapExist = false;
                    if (Files.exists(Paths.get(fileCost))) {
                        costMapExist = true;
                    }
                    DataInputStream brCost = null;
                    DataOutputStream bwCost = null;

                    /// third, get the next state values of last round
                    float[] oldRoundNextPartitionValues = new float[planSize[totalPartitionCountNext]];
                    String fileNextValue = tempFolder + "\\values\\Value-" + totalPartitionCountNext + ".txt";
                    if (!Files.exists(Paths.get(fileNextValue))) {
                        throw new IllegalArgumentException(
                                "!Files.exists(Paths.get(fileNextValue): " + fileNextValue);
                    }
                    try (DataInputStream brValue = new DataInputStream(new BufferedInputStream(Files.newInputStream(Paths.get(fileNextValue)), bufferSize))){
                        for (int j = 0; j < oldRoundNextPartitionValues.length; j++) {
                            oldRoundNextPartitionValues[j] = brValue.readFloat();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    //now, starting the two layer iteration, this cause |S_n| * |S_(n+1)| number of iterations.
                    try {
                        if (costMapExist) {
                            brCost = new DataInputStream(new BufferedInputStream(Files.newInputStream(Paths.get(fileCost)), bufferSize));
                        } else {
                            bwCost = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(Paths.get(fileCost)), bufferSize));
                        }
                        BufferedReader brCurr = Files.newBufferedReader(Paths.get(fileCurr));
                        String rdLineCurr = null;
                        int currIndex = 0;
                        while ((rdLineCurr = brCurr.readLine()) != null) {
                            float minValue = Float.MAX_VALUE;
                            String[] itemsCurr = rdLineCurr.replaceAll("\\[", "").replaceAll("\\]", "").split(",");
                            int[] srcPartition = new int[itemsCurr.length];
                            for (int j = 0; j < itemsCurr.length; j++) {
                                srcPartition[j] = Integer.parseInt(itemsCurr[j].trim());
                            }
                            BufferedReader brNext = Files.newBufferedReader(Paths.get(fileNext));
                            String rdLineNext = null;
                            int nextIndex = 0;
                            while ((rdLineNext = brNext.readLine()) != null) {
                                String[] itemsNext = rdLineNext.replaceAll("\\[", "").replaceAll("\\]", "").split(",");
                                int[] dstPartition = new int[itemsNext.length];
                                for (int j = 0; j < itemsNext.length; j++) {
                                    dstPartition[j] = Integer.parseInt(itemsNext[j].trim());
                                }

                                float remain = 0.0f;
                                if (costMapExist) {
                                    remain = brCost.readFloat();
                                } else {
                                    remain = (float) packGain(convertPack(srcPartition), convertPack(dstPartition), km);
                                    bwCost.writeFloat(remain);
                                }

                                //v_2,j = 0.2 * min_k [d(s_2,j, s_3,k) + \gamma * v_3,k]
                                float cost = (float) totalDataSize - remain + gamma * oldRoundNextPartitionValues[nextIndex];
                                if (cost < minValue) {
                                    minValue = cost;
                                    currDecisionForNextPartition[currIndex] = nextIndex;
                                }
                                nextIndex++;
                            }
                            brNext.close();
                            //for each s \in S_n, save the minimum cost
                            //v_2,j = 0.2 * min_k [d(s_2,j, s_3,k) + \gamma * v_3,k]
                            newRoundPartitionValues[currIndex] += (float) prob * minValue;
                            currIndex++;
                        }
                        brCurr.close();
                        //save the computation of each d(s1, s2)
                        if (costMapExist) {
                            brCost.close();
                        } else {
                            bwCost.close();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }//end for (Map.Entry<Integer, Double> entry : possibleState.entrySet())

                //at last, read values of last round, update diff!
                //float[] oldPartitionValues = new float[planSize[totalPartitionCountCurr]];
                String fileCurrValue = tempFolder + "\\values\\Value-" + totalPartitionCountCurr + ".txt";
                if (!Files.exists(Paths.get(fileCurrValue))) {
                    throw new IllegalArgumentException(
                            "!Files.exists(Paths.get(fileCurrValue): " + fileCurrValue);
                }
                try (DataInputStream brValue = new DataInputStream(new BufferedInputStream(Files.newInputStream(Paths.get(fileCurrValue)), bufferSize))) {
                    for (int j = 0; j <newRoundPartitionValues.length; j++) {
                        float f = brValue.readFloat();
                        diff = diff + Math.abs(newRoundPartitionValues[j] - f);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

                try {
                    Files.deleteIfExists(Paths.get(fileCurrValue));
                } catch (IOException e) {
                    e.printStackTrace();
                }

                try (DataOutputStream bwValue = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(Paths.get(fileCurrValue)), bufferSize))) {
                    for (int j = 0; j < planSize[totalPartitionCountCurr]; j++) {
                        bwValue.writeFloat(newRoundPartitionValues[j]);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("in round: " + iterates + ", finish update value vector of:  " + totalPartitionCountCurr);
            }//end for (int i = 0; i < stateValueSet.size(); i++)
            if (diff < epsilon) {
                break;
            }
            iterates++;
            System.out.println("iterates: " + iterates + ", diff: " + diff + ", target epsilon: " + epsilon);
        }//end while (iterates <= maxIteration)

        if (iterates > maxIteration) {
            System.out.println("Warning quit because of meeting max iterations.");
        } else {
            System.out.println("Iteration ends with diff: " + diff + ", target epsilon: " + epsilon);
        }
    }

    /**
     * After running the training algorithm, find all the values of each partition
     * Then follow the state transition path, we make use of this value information
     * This version does not need Decision files, but accessing value files
     * But suffice more computation.
     *
     * @param states
     * @param km
     * @param tempFolder
     * @return
     */
    private float calcOptimalMatrixWithFileNoDecisionFile(int[] states, KuhnMunkres km, String tempFolder, float gamma) {
        int[] init = PackingAlg.calc(workload, states[0]);
        float totalCost = 0.0f;

        int[] previousParitition = init;
        for (int i = 0; i < states.length - 1; i++) {
            int totalPartitionCountCurr = states[i];
            int totalPartitionCountNext = states[i + 1];

            String fileCurr = tempFolder + "\\State-" + totalPartitionCountCurr + ".txt";
            if (!Files.exists(Paths.get(fileCurr))) {
                throw new IllegalArgumentException(
                        "!Files.exists(Paths.get(fileCurr): " + fileCurr);
            }

            try {
                BufferedReader brCurr = Files.newBufferedReader(Paths.get(fileCurr));
                String rdLineCurr = null;
                int[] srcPartition = null;
                int[] dstPartition = null;
                while ((rdLineCurr = brCurr.readLine()) != null) {
                    String[] itemsCurr = rdLineCurr.replaceAll("\\[", "").replaceAll("\\]", "").split(",");
                    srcPartition = new int[itemsCurr.length];
                    for (int j = 0; j < itemsCurr.length; j++) {
                        srcPartition[j] = Integer.parseInt(itemsCurr[j].trim());
                    }

                    if (Arrays.equals(srcPartition, previousParitition)) {
                        //System.out.println("in step: " + i + ", find partition: " +
                        //        Arrays.toString(previousParitition) + "," + Arrays.toString(srcPartition) +
                        //        ", and next state and decision: " + totalPartitionCountNext);
                        break;
                    }
                }
                brCurr.close();

                String fileNext = tempFolder + "\\State-" + totalPartitionCountNext + ".txt";
                if (!Files.exists(Paths.get(fileNext))) {
                    throw new IllegalArgumentException(
                            "!Files.exists(Paths.get(fileNext): " + fileNext);
                }
                int partitionSize = getCount(fileNext);

                /// read values
                float[] nextPartitionValues = new float[partitionSize];
                String fileNextValue = tempFolder + "\\values\\Value-" + totalPartitionCountNext + ".txt";
                if (!Files.exists(Paths.get(fileNextValue))) {
                    throw new IllegalArgumentException(
                            "!Files.exists(Paths.get(fileNextValue): " + fileNextValue);
                }

                try (DataInputStream brValue = new DataInputStream(new BufferedInputStream(Files.newInputStream(Paths.get(fileNextValue)), bufferSize))) {
                    for (int j = 0; j <nextPartitionValues.length; j++) {
                        nextPartitionValues[j] = brValue.readFloat();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

                BufferedReader brNext = Files.newBufferedReader(Paths.get(fileNext));
                float minValue = Float.MAX_VALUE;
                float realCost = 0.0f;
                int nextIndex = 0;
                String rdLineNext = null;
                while ((rdLineNext = brNext.readLine()) != null) {
                    //find the target partition.
                    String[] itemsNext = rdLineNext.replaceAll("\\[", "").replaceAll("\\]", "").split(",");
                    dstPartition = new int[itemsNext.length];
                    for (int j = 0; j < itemsNext.length; j++) {
                        dstPartition[j] = Integer.parseInt(itemsNext[j].trim());
                    }

                    float cost = (float) (totalDataSize - packGain(convertPack(srcPartition), convertPack(dstPartition), km));
                    float val = cost + nextPartitionValues[nextIndex] * gamma;

                    if (val < minValue) {
                        minValue = val;
                        previousParitition = dstPartition;
                        realCost = cost;
                    }
                    nextIndex++;
                }
                brNext.close();
                totalCost += realCost;

            } catch (IOException e) {
                e.printStackTrace();
            }
            if (i % 10 == 0) {
                System.out.println("Finish: " + i);
            }
        }//end for (int i = 0; i < states.length - 1; i++)
        ///finish iteration
        return totalCost;
    }

    private int getCount(String fileName) {
        if (!Files.exists(Paths.get(fileName))) {
            throw new IllegalArgumentException("File not exist: " + fileName);
        }
        int count = 0;
        try {
            BufferedReader br = Files.newBufferedReader(Paths.get(fileName));
            String rdLine = null;
            while ((rdLine = br.readLine()) != null) {
                count++;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return count;
    }

    private void split(int start, int[] splits, int part, int totalNumPartitions,
                       double loadUpperLimit, double[] normalizedWordloads, List<int[]> feasiblePartitionPlan) {
        if (start + part > workload.length) {
            return;
        }
        if (part == 1) {
            if (start == workload.length || splits.length + 1 != totalNumPartitions) {
                throw new IllegalStateException("start == workloads.length");
            } else if (totalWorkload(start, workload.length, normalizedWordloads) <= loadUpperLimit) {
                int[] newPack = new int[splits.length + 1];
                System.arraycopy(splits, 0, newPack, 0, splits.length);
                newPack[newPack.length - 1] = workload.length - start;
                //double g = totalGain(convertPack(newPack));
                //if (g > gain) {
                //    gain = g;
                //    pack = newPack;
                //}
                feasiblePartitionPlan.add(newPack);
            }
            return;
        }
        int[] newSplits = new int[splits.length + 1];
        System.arraycopy(splits, 0, newSplits, 0, splits.length);
        for (int i = start + 1; i < workload.length; i++) {
            if (totalWorkload(start, i, normalizedWordloads) > loadUpperLimit) {
                break;
            }
            newSplits[newSplits.length - 1] = i - start;
            split(i, newSplits, part - 1, totalNumPartitions, loadUpperLimit, normalizedWordloads, feasiblePartitionPlan);
        }
    }

    private void split(int start, int[] splits, int part, int totalNumPartitions,
                       double loadUpperLimit, double[] normalizedWordloads, BufferedWriter bw) {
        if (start + part > workload.length) {
            return;
        }
        if (part == 1) {
            if (start == workload.length || splits.length + 1 != totalNumPartitions) {
                throw new IllegalStateException("start == workloads.length");
            } else if (totalWorkload(start, workload.length, normalizedWordloads) <= loadUpperLimit) {
                int[] newPack = new int[splits.length + 1];
                System.arraycopy(splits, 0, newPack, 0, splits.length);
                newPack[newPack.length - 1] = workload.length - start;
                //double g = totalGain(convertPack(newPack));
                //if (g > gain) {
                //    gain = g;
                //    pack = newPack;
                //}
                try {
                    bw.write(Arrays.toString(newPack));
                    bw.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return;
        }
        int[] newSplits = new int[splits.length + 1];
        System.arraycopy(splits, 0, newSplits, 0, splits.length);
        for (int i = start + 1; i < workload.length; i++) {
            if (totalWorkload(start, i, normalizedWordloads) > loadUpperLimit) {
                break;
            }
            newSplits[newSplits.length - 1] = i - start;
            split(i, newSplits, part - 1, totalNumPartitions, loadUpperLimit, normalizedWordloads, bw);
        }
    }


    private double getLoadUpperLimit(int numPartitions) {
        return DoubleStream.of(workload).sum() / numPartitions * ratio;
    }

    private double totalWorkload(int wStart, int wEnd, double[] normalizedWordloads) {
        double sum = 0;
        for (int i = wStart; i < wEnd; i++) {
            sum += normalizedWordloads[i];
        }
        return sum;
    }

    private void clearTmpFiles(String tempFolder, boolean clearFile) {
        File folder = new File(tempFolder);
        File[] listOfFiles = folder.listFiles();
        if (clearFile) {
            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile()) {
                    listOfFiles[i].delete();
                }
            }
        }
    }
}


