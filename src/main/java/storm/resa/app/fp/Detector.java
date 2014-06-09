package storm.resa.app.fp;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 14-6-5.
 */
public class Detector extends BaseRichBolt implements Constant {

    private static class Entry {
        int count = 0;
        boolean detectedBySelf;
        int refCount = 0;

        public void setDetectedBySelf(boolean detectedBySelf) {
            this.detectedBySelf = detectedBySelf;
        }

        public boolean isDetectedBySelf() {
            return detectedBySelf;
        }

        int incCountAndGet() {
            return ++count;
        }

        int decCountAndGet() {
            return --count;
        }

        int getRefCount() {
            return refCount;
        }

        void incRefCount() {
            refCount++;
        }

        void decRefCount() {
            refCount--;
            if (refCount == 0) {
                setDetectedBySelf(false);
            }
        }

        boolean unused() {
            return count == 0 && refCount == 0;
        }
    }

    private Map<WordList, Entry> patterns;
    private int threshold;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        patterns = new HashMap<>();
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        WordList pattern = (WordList) input.getValueByField(PATTERN_FIELD);
        Entry entry = patterns.computeIfAbsent(pattern, (k) -> new Entry());
        if (!input.getSourceStreamId().equals(FEEDBACK_STREAM)) {
            if (input.getBooleanByField(IS_ADD_FIELD)) {
                if (entry.incCountAndGet() == threshold && entry.getRefCount() == 0) {
                    //detect a new pattern
                    entry.incRefCount();
                    entry.setDetectedBySelf(true);
                    emitSubPattern(pattern.getWords(), collector, true);
                    collector.emit(Arrays.asList(pattern));
                }
            } else {
                if (entry.decCountAndGet() == threshold - 1 && entry.isDetectedBySelf()) {
                    entry.decRefCount();
                    // decrease sub pattern
                    emitSubPattern(pattern.getWords(), collector, false);
                }
            }
        } else {
            if (input.getBoolean(1)) {
                entry.incRefCount();
            } else {
                entry.decRefCount();
            }
        }
        if (entry.unused()) {
            patterns.remove(pattern);
        }
        collector.ack(input);
    }

    private void emitSubPattern(int[] wordIds, OutputCollector collector, boolean isAdd) {
        int n = wordIds.length;
        int[] buffer = new int[n];
        for (int i = 1; i < (1 << n); i++) {
            int k = 0;
            for (int j = 0; j < n; j++) {
                if ((i & (1 << j)) > 0) {
                    buffer[k++] = wordIds[j];
                }
            }
            collector.emit(FEEDBACK_STREAM, Arrays.asList(new WordList(Arrays.copyOf(buffer, k)), isAdd));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(PATTERN_FIELD));
        declarer.declareStream(FEEDBACK_STREAM, new Fields(PATTERN_FIELD, IS_ADD_FIELD));
    }
}
