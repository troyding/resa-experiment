package storm.resa.simulate;

import backtype.storm.metric.api.IMetric;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 14-1-27.
 */
public class TupleCompletedMetric implements IMetric {

    private Map<String, Long> paddingTuples = new HashMap<String, Long>();
    private Map<String, Long> completedTuples = new HashMap<String, Long>();

    public void tupleStarted(String tupleId) {
        paddingTuples.put(tupleId, System.currentTimeMillis());
    }

    public void tupleFailed(String tupleId) {
        paddingTuples.remove(tupleId);
        completedTuples.put(tupleId, -1L);
    }

    public void tupleCompleted(String tupleId) {
        // move tuple from padding to completed
        Long startTime = paddingTuples.remove(tupleId);
        completedTuples.put(tupleId, System.currentTimeMillis() - startTime);
    }

    @Override
    public Object getValueAndReset() {
        Map<String, Long> ret = completedTuples;
        completedTuples = new HashMap<String, Long>();
        return ret;
    }

}
