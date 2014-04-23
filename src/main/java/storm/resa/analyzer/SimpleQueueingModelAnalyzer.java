package storm.resa.analyzer;

import storm.resa.util.ConfigUtil;

import java.util.*;

/**
 * Created by Tom.fu on 23/4/2014.
 * Chain topology and not tuple split
 */
public class SimpleQueueingModelAnalyzer {

    public static double estCompleteTime(Map<String, QueueNode> components, boolean printDetail, boolean showMinReq) {
        double completeTime = 0.0;
        if (!checkStable(components, false)) {
            System.out.println("Unstable detected: ");
            checkStable(components, true);
            return Double.MAX_VALUE;
        }

        for (Map.Entry<String, QueueNode> e : components.entrySet()) {
            double estT = e.getValue().estErlangT();
            if (printDetail) {
                System.out.println(e.getKey() + String.format(", estT: %.5f, ", estT) + e.getValue().queueNodeKeyStats(showMinReq));
            }
            completeTime += estT;
        }
        return completeTime;
    }

    public static boolean checkStable (Map<String, QueueNode> components, boolean printDetail) {
        boolean ret = true;
        for (Map.Entry<String, QueueNode> e : components.entrySet()) {
            boolean s = e.getValue().isStable();
            if (printDetail) {
                System.out.println(e.getKey() + ", stable: " + s + ", " + e.getValue().queueNodeKeyStats(false));
            }
            ret = ret & s;
        }
        return ret;
    }

    public static void checkOptimized(Map<String, QueueNode> components, Map<String, Object> para, boolean printDetail) {

        int maxThreadAvailable4Bolt = ConfigUtil.getInt(para, "maxThreadAvailable4Bolt", components.size());
        int minReq = getTotalMinRequirement(components);
        System.out.println("MinReq: " + minReq + ", current total available: " + maxThreadAvailable4Bolt);

        if (minReq >= maxThreadAvailable4Bolt) {
            System.out.println("Resource is not adequate, therefore, system is not stable and try to find bottleneck first");
            return;
        }

        Map<String, Integer> curr = getAllocation(components, para);
        Map<String, Integer> after = suggestAllocation(components, maxThreadAvailable4Bolt - minReq, printDetail);

        System.out.println("---------------------- Current Allocation ----------------------");
        printAllocation(curr);
        System.out.println("---------------------- Suggested Allocation ----------------------");
        printAllocation(after);
    }

    public static int getTotalMinRequirement(Map<String, QueueNode> components) {
        int totalMinReq = 0;
        for (Map.Entry<String, QueueNode> e : components.entrySet()) {
            int minReq = e.getValue().getMinServerCount();
            if ( minReq < Integer.MAX_VALUE) {
                totalMinReq += minReq;
            } else {
                return Integer.MAX_VALUE;
            }
        }
        return totalMinReq;
    }

    public static Map<String, Integer> getAllocation(Map<String, QueueNode> components, Map<String, Object> para) {
        Map<String, Integer> retVal = new HashMap<>();
        for (Map.Entry<String, QueueNode> e : components.entrySet()) {
            String cid = e.getKey();
            int curr = ConfigUtil.getInt(para, cid, 0);
            retVal.put(cid, curr);
        }
        return retVal;
    }

    public static Map<String, Integer> suggestAllocation(Map<String, QueueNode> components, int remainCount, boolean printDetail) {
        Map<String, Integer> retVal = new HashMap<>();

        String defaultCID = null;

        for (Map.Entry<String, QueueNode> e : components.entrySet()) {
            String cid = e.getKey();
            defaultCID = cid;
            int minReq = e.getValue().getMinServerCount();

            retVal.put(cid, minReq);
        }

        if (remainCount > 0) {
            for (int i = 0; i < remainCount; i++) {
                double maxDiff = 0.0;
                String maxDiffCid = defaultCID;

                for (Map.Entry<String, QueueNode> e : components.entrySet()) {
                    String cid = e.getKey();
                    QueueNode qn = e.getValue();
                    int currentAllocated = retVal.get(cid);

                    double beforeAddT = QueueNode.estErlangT(qn.getLambda(), qn.getMu(), currentAllocated);
                    double afterAddT = QueueNode.estErlangT(qn.getLambda(), qn.getMu(), currentAllocated + 1);

                    double diff = beforeAddT - afterAddT;
                    if (diff > maxDiff) {
                        maxDiff = diff;
                        maxDiffCid = cid;
                    }
                }

                int newAllocate = retVal.get(maxDiffCid) + 1;
                retVal.put(maxDiffCid, newAllocate);

                if (printDetail) {
                    System.out.println((i+1) + " of " + remainCount + ", assigned to " + maxDiffCid + ", newAllocate: " + newAllocate);
                }
            }
        }

        return retVal;
    }

    public static void printAllocation(Map<String, Integer> allocation) {
        for (Map.Entry<String, Integer> e : allocation.entrySet()) {
            System.out.println(e.getKey() + ": " + e.getValue());
        }
    }
}
