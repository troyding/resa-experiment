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
public class DetectorNew3 extends BaseRichBolt implements Constant {

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

        String reportCnt() {
            return String.format(" cnt: %d, refCnt: %d", this.count, this.refCount);
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
        System.out.println(
                "In DetectorNew, threshold: " + threshold);
    }

    /////////////////// State Transition Graph, Implementation II//////////////////////////////
    /// States: ( Cnt > Threshold ? , hasReference? | isMaxFreqPattern? ) Stable state      ///
    ///         [   ,   |  ] Temp State -->  trigger update                                 ///
    /// Event and Output: Cnt (+/-), pattern count inc/dec (DefaultStream)                  ///
    ///                   RefCnt (+/-), reference count update from (Feedback Stream)       ///
    ///                   Output to next bolt: *T/F*                                        ///
    ///                                                                                     ///
    ///                 (        ) ----- (Cnt+ *T*) -----> (        )                       ///
    ///                 (F, F | F) <---- (Cnt- *F*) ------ (T, F | T)<--------------+       ///
    ///                    |   ^                              |   ^                 |       ///
    ///           (RefCnt-)|   | (RefCnt+)           (RefCnt-)|   | (RefCnt+)       |       ///
    ///                    V   |                              V   |                 |       ///
    ///                 (F, T | F )                        [T, T | T ]              |       ///
    ///                    |   ^                              |                     |       ///
    ///              (Cnt-)|   | (Cnt+)                       |                     |       ///
    ///                    V   |                              |                     |       ///
    ///                 (T, T | F ) <----(trigger update )----+                     |       ///
    ///                    |   ^                                                    |       ///
    ///           (RefCnt-)|   | (RefCnt+)                                          |       ///
    ///                    V   |                                                    |       ///
    ///                 [T, F | F ]-------------------(trigger update)--------------+       ///
    ///                                                                                     ///
    ///////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public void execute(Tuple input) {
        WordList pattern = (WordList) input.getValueByField(PATTERN_FIELD);
        Entry entry = patterns.computeIfAbsent(pattern, (k) -> new Entry());

        if (!input.getSourceStreamId().equals(FEEDBACK_STREAM)) {
            ///Pattern Count Stream, only affect pattern count
            ///We only change IncRef and DecRef at two events: [++count == threshold] and [--count == threshold-1]
            if (input.getBooleanByField(IS_ADD_FIELD)) {
                entry.incCountAndGet();
                System.out.println(
                        "In DetectorNew(default), cntInc: " + pattern + "," + entry.reportCnt());
                if (entry.getCount() == threshold) {
                    incRefToSubPatternExcludeSelf(pattern.getWords(), collector, input);
                    System.out.println(
                            "In DetectorNew(default), cntInc: " + pattern + ",satisfy thresh and incRef");
                }
            } else {
                entry.decCountAndGet();
                System.out.println(
                        "In DetectorNew(default), cntDec: " + pattern + "," + entry.reportCnt());
                if (entry.getCount() == threshold - 1) {
                    decRefToSubPatternExcludeSelf(pattern.getWords(), collector, input);
                    System.out.println(
                            "In DetectorNew(default), cntDec: " + pattern + ",dissatisfy thresh and decRef");
                }
            }

            ///We separate the action of update refCount and update states
            if (!entry.isMFPattern()) {///entry.isMFPattern == false
                if (entry.getCount() >= threshold && !entry.hasReference()) {
                    ///State (F, F | F) -> (T, F | T),
                    ///State (T, F | F) -> (T, F | T),
                    ///[output pattern, T]
                    entry.setMFPattern(true);
                    collector.emit(REPORT_STREAM, input, Arrays.asList(pattern, true));
                    System.out.println(
                            "In DetectorNew(default), set isMFP" + pattern + "," + entry.reportCnt());
                }
            } else {///entry.isMFPattern == true
                if (entry.hasReference() || (!entry.hasReference() && entry.getCount() < threshold)) {
                    ///State (T, T | T) -> (F, T | F)
                    ///State (T, T | T) -> (T, T | F)
                    ///State (T, F | T) -> (F, F | F)
                    ///[output pattern, F]
                    entry.setMFPattern(false);
                    collector.emit(REPORT_STREAM, input, Arrays.asList(pattern, false));

                    System.out.println(
                            "In DetectorNew(default), cancel isMFP" + pattern + "," + entry.reportCnt());
                }
            }
        } else {
            ///Feedback_STREAM, only affect refCount, also check states.
            ///State (F, F | F) <--> (F, T | F)
            ///State (T, F | F) <--> (T, T | F)
            ///State (T, F | T) <--> (T, T | T)
            if (input.getBooleanByField(IS_ADD_FIELD)) {
                entry.incRefCountAndGet();
                System.out.println(
                        "In DetectorNew(FB), incReferenceCnt: " + pattern + ", " + entry.reportCnt());
            } else {
                entry.decRefCountAndGet();
                System.out.println(
                        "In DetectorNew(FB), DecReferenceCnt: " + pattern + ", " + entry.reportCnt());
            }

            ///TODO: please double check if we need to anchor tuple here?
            ///update states
            if (entry.hasReference() && entry.isMFPattern()) {
                ///State [*, T | T] --> (*, T | F), update states and output F
                entry.setMFPattern(false);
                collector.emit(REPORT_STREAM, input, Arrays.asList(pattern, false));
                System.out.println(
                        "In DetectorNew(FB), cancel isMFP: " + pattern + ", " + entry.reportCnt());
            } else if (!entry.hasReference() && !entry.isMFPattern() && entry.getCount() >= threshold) {
                ///State [T, F | F] --> (T, F | T) update states and output T
                entry.setMFPattern(true);
                collector.emit(REPORT_STREAM, input, Arrays.asList(pattern, true));
                System.out.println(
                        "In DetectorNew(FB), set isMFP" + pattern + "," + entry.reportCnt());
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
