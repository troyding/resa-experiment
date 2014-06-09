package storm.resa.app.fp;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import storm.resa.util.ConfigUtil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 14-6-5.
 */
public class DetectorNew extends BaseRichBolt implements Constant {

    private static class Entry {
        int count = 0;
        boolean detectedBySelf;
        int refCount = 0;
        boolean flagMFPattern = false;

        public void setDetectedBySelf(boolean detectedBySelf) {
            this.detectedBySelf = detectedBySelf;
        }

        public boolean isDetectedBySelf() {
            return detectedBySelf;
        }

        public void setMFPattern(boolean flag) {
            this.flagMFPattern = flag;
        }

        public boolean isMFPattern() {
            return this.flagMFPattern;
        }

        int getCount() {
            return count;
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

        int incRefCountAndGet() {
            return ++refCount;
        }

        int decRefCountAndGet() {
            return --refCount;
        }

        boolean hasReference() {
            return this.refCount > 0;
        }

        boolean unused() {
            return count <= 0 && refCount <= 0;
        }
    }

    private Map<WordList, Entry> patterns;
    private int threshold;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.patterns = new HashMap<>();
        this.collector = collector;
        this.threshold = ConfigUtil.getInt(stormConf, THRESHOLD_PROP, 20);
    }

    ////////////////////////////// State Transition Graph /////////////////////////////////////
    /// States: ( Cnt > Threshold ? , hasReference? | isMaxFreqPattern? )                   ///
    /// Event and Output: Cnt (+/-), pattern count inc/dec (DefaultStream)                  ///
    ///                   RefCnt (+/-), reference count update from (Feedback Stream)       ///
    ///                   Output to next bolt: *T/F*                                        ///
    ///                                                                                     ///
    ///                 (        ) ----- (Cnt+ *T*) -----> (        )                       ///
    ///      +--------->(F, F | F) <---- (Cnt- *F*) ------ (T, F | T)<--------------+       ///
    ///      |             |   ^                              |   ^                 |       ///
    ///      |    (RefCnt-)|   | (RefCnt+)           (RefCnt-)|   | (RefCnt+)       |       ///
    ///      |             V   |                              V   |                 |       ///
    ///      |          (F, T | F ) <----(Cnt- *F*)------- (T, T | T )              |       ///
    ///      |             |   ^                              |                     |       ///
    ///      |       (Cnt-)|   | (Cnt+)                       |                     |       ///
    ///      |             V   |                              |                     |       ///
    ///      |          (T, T | F ) <----(Cnt+ *F*)-----------+                     |       ///
    ///      |             |   ^                                                    |       ///
    ///      |    (RefCnt-)|   | (RefCnt+)                                          |       ///
    ///      |             V   |                                                    |       ///
    ///      +----------(T, F | F )-------------------------------------------------+       ///
    ///         (Cnt-)                                  (Cnt+ *T*)                          ///
    ///                                                                                     ///
    ///////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public void execute(Tuple input) {
        WordList pattern = (WordList) input.getValueByField(PATTERN_FIELD);
        Entry entry = patterns.computeIfAbsent(pattern, (k) -> new Entry());

        if (!input.getSourceStreamId().equals(FEEDBACK_STREAM)) {
            ///Pattern Count Stream, only affect pattern count
            if (input.getBooleanByField(IS_ADD_FIELD)) {
                entry.incCountAndGet();
            } else {
                entry.decCountAndGet();
            }

            if (!entry.isMFPattern()) {///entry.isMFPattern == false
                if (entry.getCount() >= threshold && !entry.hasReference()) {
                    ///State (F, F | F) -> (T, F | T),
                    ///State (T, F | F) -> (T, F | T),
                    ///[output pattern, T]
                    entry.setMFPattern(true);
                    collector.emit(REPORT_STREAM, input, Arrays.asList(pattern, true));
                    incRefToSubPatternExcludeSelf(pattern.getWords(), collector, input);
                }
            } else {///entry.isMFPattern == true
                if (entry.hasReference() || (!entry.hasReference() && entry.getCount() < threshold)) {
                    ///State (T, T | T) -> (F, T | F)
                    ///State (T, T | T) -> (T, T | F)
                    ///State (T, F | T) -> (F, F | F)
                    ///[output pattern, F]
                    entry.setMFPattern(false);
                    collector.emit(REPORT_STREAM, input, Arrays.asList(pattern, false));
                    decRefToSubPatternExcludeSelf(pattern.getWords(), collector, input);
                }
            }
        } else {
            ///Feedback_STREAM, only affect refCount
            ///State (F, F | F) <--> (F, T | F)
            ///State (T, F | F) <--> (T, T | F)
            ///State (T, F | T) <--> (T, T | T)
            ///No output
            if (input.getBooleanByField(IS_ADD_MFP)) {
                entry.incRefCountAndGet();
            } else {
                entry.decRefCountAndGet();
            }
        }
        if (entry.unused()) {
            patterns.remove(pattern);
        }
        collector.ack(input);
    }

    private void incRefToSubPatternExcludeSelf(int[] wordIds, OutputCollector collector, Tuple input) {
        adjRefToSubPatternExcludeSelf(wordIds, collector, input, true);
    }

    private void decRefToSubPatternExcludeSelf(int[] wordIds, OutputCollector collector, Tuple input) {
        adjRefToSubPatternExcludeSelf(wordIds, collector, input, false);
    }

    private void adjRefToSubPatternExcludeSelf(int[] wordIds, OutputCollector collector, Tuple input, boolean adj) {
        int n = wordIds.length;
        int[] buffer = new int[n];
        ///Note that here we exclude itself as one of the sub-patterns
        ///for (int i = 1; i < (1 << n); i++) {
        for (int i = 1; i < (1 << n) - 1; i++) {
            int k = 0;
            for (int j = 0; j < n; j++) {
                if ((i & (1 << j)) > 0) {
                    buffer[k++] = wordIds[j];
                }
            }
            collector.emit(FEEDBACK_STREAM, input, Arrays.asList(new WordList(Arrays.copyOf(buffer, k)), adj));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(REPORT_STREAM, new Fields(PATTERN_FIELD, IS_ADD_MFP));
        declarer.declareStream(FEEDBACK_STREAM, new Fields(PATTERN_FIELD, IS_ADD_FIELD));
    }
}
