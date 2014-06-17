package storm.resa.migrate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

/**
 * Created by ding on 14-6-11.
 */
public class PackCalculator {
    private static class Range {
        final int start;
        final int end;

        Range(int start, int end) {
            this.start = start;
            this.end = end;
        }

        boolean contains(int v) {
            return start <= v && v <= end;
        }
    }

    private static class Pack {
        int[] packing;
        double gain;

        Pack(int[] packing, double sumOfDiffSquare) {
            this.packing = packing;
            this.gain = sumOfDiffSquare;
        }
    }

    private static final Pack INFEASIBLE = new Pack(new int[0], Double.MIN_VALUE);

    private double[] workloads;
    private double[] normalizedWordloads;
    private double[] dataSizes;
    private Range[] currPacks;
    private int targetNumPacks;
    private Map<String, Pack> cache;
    private float ratio = 1.3f;
    private double loadUpperLimit;
    private KuhnMunkres kmAlg;

    // cache result
    private Pack result = null;

    public int[] getPacks() {
        Objects.requireNonNull(result, "Calc is not called");
        return result == INFEASIBLE ? null : result.packing;
    }

    public Double dataRemain() {
        Objects.requireNonNull(result, "Calc is not called");
        return result == INFEASIBLE ? null : result.gain;
    }

    public PackCalculator setTargetNumPacks(int targetNumPacks) {
        this.targetNumPacks = targetNumPacks;
        return this;
    }

    public PackCalculator setUpperLimitRatio(float ratio) {
        if (Float.compare(1f, ratio) >= 0) {
            throw new IllegalArgumentException("Bad ratio: " + ratio);
        }
        this.ratio = ratio;
        return this;
    }

    public PackCalculator setDataSizes(double[] dataSizes) {
        this.dataSizes = dataSizes;
        return this;
    }

    public PackCalculator setCurrPacks(int[] packs) {
        this.currPacks = new Range[packs.length];
        int start = 0;
        for (int i = 0; i < packs.length; i++) {
            int end = start + packs[i];
            this.currPacks[i] = new Range(start, end - 1);
            start = end;
        }
        return this;
    }

    public PackCalculator setWorkloads(double[] workloads) {
        this.workloads = workloads;
        return this;
    }

    private void checkAndInit() {
        if (workloads.length < targetNumPacks) {
            throw new IllegalArgumentException("targetNumPacks is larger than workload size");
        }
        if (targetNumPacks == currPacks.length) {
            throw new IllegalArgumentException("Current number of packs equals targetNumPacks");
        }
        if (workloads.length != currPacks[currPacks.length - 1].end + 1) {
            throw new IllegalArgumentException("currPacks mismatch with workload");
        }
        if (workloads.length != dataSizes.length) {
            throw new IllegalArgumentException("workloads.length != dataSizes.length");
        }
        loadUpperLimit = DoubleStream.of(workloads).sum() / targetNumPacks * ratio;
        normalizedWordloads = new double[workloads.length];
        for (int i = 0; i < normalizedWordloads.length; i++) {
            if (workloads[i] > loadUpperLimit) {
                normalizedWordloads[i] = loadUpperLimit;
            } else {
                normalizedWordloads[i] = workloads[i];
            }
        }
    }

    public PackCalculator calc() {
        checkAndInit();
        result = calcPack();
        return this;
    }

    private Pack calcPack() {
        kmAlg = new KuhnMunkres(Math.max(targetNumPacks, currPacks.length));
        Pack p;
        if (normalizedWordloads.length == targetNumPacks) {
            int[] ret = new int[normalizedWordloads.length];
            Arrays.fill(ret, 1);
            Range[] packs = IntStream.range(0, targetNumPacks).mapToObj(i -> new Range(i, i)).toArray(Range[]::new);
            p = new Pack(ret, gain(packs, 0, currPacks.length));
        } else {
            //init buffer
            cache = new HashMap<>();
            p = calc0(0, normalizedWordloads.length, targetNumPacks, 0, currPacks.length);
            // release memory
            cache = null;
        }
        kmAlg = null;
        normalizedWordloads = null;
        return p;
    }

    private Pack calc0(int wStart, int wEnd, int numPartition, int pStart, int pEnd) {
        if (wEnd - wStart < numPartition || numPartition == 0) {
            throw new IllegalStateException("start=" + wStart + ", end=" + wEnd + ", numPartition=" + numPartition);
        }
        String key = wStart + "-" + wEnd + "-" + numPartition + "-" + pStart + "-" + pEnd;
        return cache.computeIfAbsent(key, k -> calc1(wStart, wEnd, numPartition, pStart, pEnd));
    }

    private Pack calc1(int wStart, int wEnd, int numPartition, int pStart, int pEnd) {
        if (numPartition == 1) {
            double sum = totalWorkload(wStart, wEnd);
            return sum > loadUpperLimit ? INFEASIBLE :
                    new Pack(new int[]{wEnd - wStart}, gain(new Range[]{new Range(wStart, wEnd - 1)}, pStart, pEnd));
        } else if (wEnd - wStart == numPartition) {
            int[] ret = new int[numPartition];
            Arrays.fill(ret, 1);
            Range[] newRanges = IntStream.range(wStart, wEnd).mapToObj(i -> new Range(i, i)).toArray(Range[]::new);
            return new Pack(ret, gain(newRanges, pStart, pEnd));
        }
        int split = wStart + 1;
        Pack rightPack = INFEASIBLE;
        double gain = Double.MIN_VALUE;
        for (int i = split; i < wEnd; i++) {
            if (totalWorkload(wStart, i) > loadUpperLimit || wEnd - i < numPartition - 1) {
                continue;
            }
            int startPack = findPack(split);
            int stopPack = currPacks[startPack].start < split ? startPack + 1 : startPack;
            startPack = Math.max(startPack, pStart);
            stopPack = Math.min(stopPack + 1, pEnd);
            for (int k = startPack; k < stopPack; k++) {
                Pack left = calc0(wStart, i, 1, pStart, k);
                Pack right = calc0(i, wEnd, numPartition - 1, k, pEnd);
                if (left != INFEASIBLE && right != INFEASIBLE) {
                    double newGain = left.gain + right.gain;
                    if (Double.compare(newGain, gain) > 0) {
                        gain = newGain;
                        split = i;
                        rightPack = right;
                    }
                }
            }
        }
        if (rightPack == INFEASIBLE) {
            return INFEASIBLE;
        }
        int[] ret = new int[numPartition];
        ret[0] = split - wStart;
        System.arraycopy(rightPack.packing, 0, ret, 1, numPartition - 1);
        return new Pack(ret, gain);
    }

    private int findPack(int v) {
        for (int i = 0; i < currPacks.length; i++) {
            if (currPacks[i].end >= v) {
                return i;
            }
        }
        throw new IllegalStateException("value is " + v);
    }

    private double totalWorkload(int wStart, int wEnd) {
        double sum = 0;
        for (int i = wStart; i < wEnd; i++) {
            sum += normalizedWordloads[i];
        }
        return sum;
    }

    private double gain(Range[] newPacks, int pStart, int pEnd) {
        if (newPacks.length == 0 || pStart == pEnd) {
            return 0;
        }
        double[][] weights = new double[newPacks.length][pEnd - pStart];
        for (int i = 0; i < weights.length; i++) {
            for (int j = 0; j < weights[i].length; j++) {
                weights[i][j] = overlap(newPacks[i], currPacks[pStart + j]);
            }
        }
        double[] maxWeight = new double[1];
        kmAlg.getMaxBipartie(weights, maxWeight);
        // int[][] pairs = kmAlg.getMaxBipartie(weights, maxWeight);
        // return Stream.of(pairs).mapToDouble(r -> overlap(newPacks[r[0]], currPacks[pStart + r[1]])).sum();
        return maxWeight[0];
    }

    private double overlap(Range r1, Range r2) {
        if (r1.start <= r2.start && r1.end >= r2.start) {
            return IntStream.rangeClosed(r2.start, Math.min(r2.end, r1.end)).mapToDouble(i -> dataSizes[i]).sum();
        } else if (r1.start >= r2.start && r1.start <= r2.end) {
            return IntStream.rangeClosed(r1.start, Math.min(r2.end, r1.end)).mapToDouble(i -> dataSizes[i]).sum();
        }
        return 0;
    }
}
