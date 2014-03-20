package storm.resa.measure;

import backtype.storm.metric.api.IMetric;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 14-1-28.
 */
public class AggExecuteMetric implements IMetric {

    private static class Accumulator {
        private int count = 0;
        private long sum = 0;
        private long sumOfSquare = 0;

        void add(int num) {
            count++;
            sum = sum + num;
            sumOfSquare = sumOfSquare + num * num;
        }

        @Override
        public String toString() {
            return count + "," + sum + "," + sumOfSquare;
        }
    }

    private Map<String, Accumulator> data = new HashMap<>();

    public void addMetric(String key, int value) {
        data.computeIfAbsent(key, (k) -> new Accumulator()).add(value);
    }

    @Override
    public Object getValueAndReset() {
        Map<String, String> ret = new HashMap<>((int) (data.size() / 0.75f) + 1);
        data.forEach((k, v) -> {
            ret.put(k, v.toString());
        });
        data.clear();
        return ret;
    }
}
