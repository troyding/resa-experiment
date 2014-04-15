package storm.resa.analyzer;

import storm.resa.util.SizeBoundedIterable;

/**
 * Created by ding on 14-4-14.
 */
public class StaticAnalyzer {

    public static void main(String[] args) {

        AggMetricAnalyzer aggAnalyzer = new AggMetricAnalyzer(new JedisResource(args[0], Integer.valueOf(args[1]), args[2]));
        aggAnalyzer.calCMVStat();
    }
}

