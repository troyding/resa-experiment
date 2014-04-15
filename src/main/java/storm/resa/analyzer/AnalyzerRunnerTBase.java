package storm.resa.analyzer;

import backtype.storm.utils.Utils;

/**
 * Created by ding on 14-4-14.
 */
public class AnalyzerRunnerTBase {

    public static void main(String[] args) {
        int timeIntervalSec = Integer.valueOf(args[3]);
        while (true) {
            // run analyze for each batch
            AggMetricAnalyzer aggAnalyzer = new AggMetricAnalyzer(new JedisResource(args[0], Integer.valueOf(args[1]), args[2]));
            aggAnalyzer.calCMVStat();

            if (!aggAnalyzer.getSpoutResult().isEmpty() || !aggAnalyzer.getBoltResult().isEmpty()) {
                System.out.println("----------- Message reported on " + System.currentTimeMillis() + " -----------------");
                AggMetricAnalyzer.printCMVStat(aggAnalyzer.getSpoutResult());
                AggMetricAnalyzer.printCMVStatShort(aggAnalyzer.getBoltResult());
            }

            long interval = timeIntervalSec * 1000;
            Utils.sleep(interval);
        }
    }
}
