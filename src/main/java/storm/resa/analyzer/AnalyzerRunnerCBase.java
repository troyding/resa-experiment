package storm.resa.analyzer;

import storm.resa.util.SizeBoundedIterable;

/**
 * Created by ding on 14-4-14.
 */
public class AnalyzerRunnerCBase {

    public static void main(String[] args) {
        Iterable<Object> dataStream = new JedisResource(args[0], Integer.valueOf(args[1]), args[2]);
        int maxBatchSize = Integer.valueOf(args[3]);
        while (true) {
            // run analyze for each batch
            AggMetricAnalyzer aggAnalyzer = new AggMetricAnalyzer(new SizeBoundedIterable<>(maxBatchSize, dataStream));
            aggAnalyzer.calCMVStat();

            if (!aggAnalyzer.getSpoutResult().isEmpty() || !aggAnalyzer.getBoltResult().isEmpty()) {
                System.out.println("----------- Message reported on " + System.currentTimeMillis() + " -----------------");
                AggMetricAnalyzer.printCMVStatShort(aggAnalyzer.getSpoutResult());
                AggMetricAnalyzer.printCMVStatShort(aggAnalyzer.getBoltResult());
            }
        }
    }
}
