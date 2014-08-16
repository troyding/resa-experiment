package storm.resa.migrate;

import java.util.*;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

/**
 * Created by ding on 14-6-11.
 */
public abstract class PackCalculator {
    protected static class Range {
        public final int start;
        public final int end;

        Range(int start, int end) {
            if (start > end) {
                throw new IllegalArgumentException("start=" + start + ", end=" + end);
            }
            this.start = start;
            this.end = end;
        }

        boolean contains(int v) {
            return start <= v && v <= end;
        }

        @Override
        public String toString() {
            return "[" + start + ","+end+"]";
        }
    }

    protected static class Pack {
        public int[] packing;
        public double gain;

        Pack(int[] packing, double gain) {
            this.packing = packing;
            this.gain = gain;
        }
    }

    protected static final Pack INFEASIBLE = new Pack(new int[0], Double.MIN_VALUE);

    protected double[] workloads;
    protected double[] normalizedWordloads;
    protected double[] dataSizes;
    protected Map.Entry<Range[], Double>[] srcPacks;
    protected int packSize;
    private float ratio = 1.3f;
    protected double loadUpperLimit;

    // cache result
    private Pack result = null;

    public int[] getPack() {
        Objects.requireNonNull(result, "Calc is not called");
        return result == INFEASIBLE ? null : result.packing;
    }

    public Double gain() {
        Objects.requireNonNull(result, "Calc is not called");
        return result == INFEASIBLE ? null : result.gain;
    }

    public PackCalculator setTargetPackSize(int packSize) {
        this.packSize = packSize;
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

    public PackCalculator setSrcPack(int[] pack) {
        return setSrcPacks(Collections.singletonMap(pack, 1.0));
    }

    /**
     * Calcuate the range values of a partition
     * pack[i] is the number of tasks belonging to the ith partition, e.g., 3 partitions with 3, 4, 5 tasks
     * Range results are 0-2, 3-6, 7-11
     *
     * @param pack
     * @return
     */
    protected static Range[] convertPack(int[] pack) {
        Range[] ret = new Range[pack.length];
        int start = 0;
        for (int i = 0; i < pack.length; i++) {
            int end = start + pack[i];
            ret[i] = new Range(start, end - 1);
            start = end;
        }
        return ret;
    }

    protected static int[] convertPack(Range[] pack) {
        return Stream.of(pack).mapToInt(p -> p.end - p.start + 1).toArray();
    }

    public PackCalculator setSrcPacks(Map<int[], Double> packs) {
        this.srcPacks = packs.entrySet().stream()
                .map(e -> new AbstractMap.SimpleEntry<>(convertPack(e.getKey()), e.getValue()))
                .toArray(Map.Entry[]::new);
        return this;
    }

    public PackCalculator setWorkloads(double[] workloads) {
        this.workloads = workloads;
        return this;
    }

    private void checkAndInit() {
        if (workloads.length < packSize) {
            throw new IllegalArgumentException("packSize is larger than workload size");
        }
        if (Stream.of(srcPacks).anyMatch(e -> e.getKey().length == packSize)) {
            throw new IllegalArgumentException("Current number of packs equals packSize");
        }
        if (!Stream.of(srcPacks).allMatch(e -> workloads.length == e.getKey()[e.getKey().length - 1].end + 1)) {
            throw new IllegalArgumentException("srcPack mismatch with workload");
        }
        if (workloads.length != dataSizes.length) {
            throw new IllegalArgumentException("workloads.length != dataSizes.length");
        }
        loadUpperLimit = DoubleStream.of(workloads).sum() / packSize * ratio;
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
        normalizedWordloads = null;
        return this;
    }

    protected abstract Pack calcPack();

}
