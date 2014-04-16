package storm.resa.analyzer;

import storm.resa.util.TimeBoundedIterable;

/**
 * Created by ding on 14-4-14.
 */
public class AnalyzerRunnerTBase {

    public static void main(String[] args) {
        Iterable<Object> dataStream = new JedisResource(args[0], Integer.valueOf(args[1]), args[2]);
        int timeIntervalSec = Integer.valueOf(args[3]);
        long duration = timeIntervalSec * 1000;
        while (true) {
            // run analyze for each time batch
            AggMetricAnalyzer aggAnalyzer = new AggMetricAnalyzer(new TimeBoundedIterable<>(duration, dataStream));
            aggAnalyzer.calCMVStat();

            if (!aggAnalyzer.getSpoutResult().isEmpty() || !aggAnalyzer.getBoltResult().isEmpty()) {
                System.out.println("----------- Message reported on " + System.currentTimeMillis() + " -----------------");
                AggMetricAnalyzer.printCMVStat(aggAnalyzer.getSpoutResult());
                AggMetricAnalyzer.printCMVStatShort(aggAnalyzer.getBoltResult());
            }
        }
    }
}
