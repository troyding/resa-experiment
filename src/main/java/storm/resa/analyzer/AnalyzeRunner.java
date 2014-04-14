package storm.resa.analyzer;

import storm.resa.util.SizeBoundedIterable;

/**
 * Created by ding on 14-4-14.
 */
public class AnalyzeRunner {

    public static void main(String[] args) {
        Iterable<Object> dataStream = new JedisResource(args[0], Integer.valueOf(args[1]), args[2]);
        int maxBatchSize = Integer.valueOf(args[3]);
        while (true) {
            // run analyze for each batch
            AggMetricAnalyzer aggAnalyzer = new AggMetricAnalyzer(new SizeBoundedIterable<>(maxBatchSize, dataStream));
            aggAnalyzer.calCMVStat();
        }
    }
}
