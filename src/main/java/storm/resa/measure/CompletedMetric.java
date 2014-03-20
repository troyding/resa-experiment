package storm.resa.measure;

import backtype.storm.metric.api.IMetric;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 14-1-27.
 */
public class CompletedMetric implements IMetric {

    private Map<String, Long> paddingTuples = new HashMap<String, Long>();
    private Map<String, Long> completedTuples = new HashMap<String, Long>();
    private boolean agg = false;

    public CompletedMetric(boolean agg) {
        this.agg = agg;
    }

    public CompletedMetric() {
        this(false);
    }

    public void tupleStarted(String traceId) {
        paddingTuples.put(traceId, System.currentTimeMillis());
    }

    public void tupleFailed(String traceId) {
        paddingTuples.remove(traceId);
        if (!agg) {
            completedTuples.put(traceId, -1L);
        }
    }

    public void tupleCompleted(String traceId) {
        // move tuple from padding to completed
        Long startTime = paddingTuples.remove(traceId);
        completedTuples.put(traceId, System.currentTimeMillis() - startTime);
    }

    @Override
    public Object getValueAndReset() {
        Map<String, Long> ret = completedTuples;
        completedTuples = new HashMap<String, Long>();
        if (agg) {
            long sum = 0, sumOfSquare = 0;
            for (Long l : ret.values()) {
                sum = sum + l;
                sumOfSquare = sumOfSquare + l * l;
            }
            return ret.size() + "," + sum + "," + sumOfSquare;
        }
        return ret;
    }

}
