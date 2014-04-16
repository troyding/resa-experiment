package storm.resa.analyzer;

import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.utils.Utils;
import storm.resa.util.TopologyHelper;

import java.util.List;
import java.util.Map;

/**
 * Created by ding on 14-4-14.
 */
public class StaticAnalyzer {

    public static void main(String[] args) {

        AggMetricAnalyzer aggAnalyzer = new AggMetricAnalyzer(new JedisResource(args[0], Integer.valueOf(args[1]), args[2]));

        aggAnalyzer.calCMVStat();

        AggMetricAnalyzer.printCMVStat(aggAnalyzer.getSpoutResult());
        AggMetricAnalyzer.printCMVStatShort(aggAnalyzer.getBoltResult());

       try {

           ///Map<String, Object> conf = TopologyStatAnalyzer.getDefaultConf();
           Map<String, Object> conf = Utils.readDefaultConfig();

           TopologyDetails td = TopologyHelper.getTopologyDetails(args[3], conf);
           TopologyStatAnalyzer.PrintTopoDetail(td);

           Map<String, List<Integer>> bolt2t = TopologyHelper.boltComponentToTasks(td);

           System.out.println("There are total: " + td.getTopology().get_bolts_size() + " bolts:");
           for (Map.Entry<String, List<Integer>> e : bolt2t.entrySet()) {
               System.out.println(e.getKey());
               e.getValue().forEach(i -> System.out.println(i));
           }

           System.out.println("There are total: " + td.getTopology().get_spouts_size() + " spouts:");
           Map<String, List<Integer>> spout2t = TopologyHelper.spoutComponentToTasks(td);
           for (Map.Entry<String, List<Integer>> e : spout2t.entrySet()) {
               System.out.println(e.getKey());
               e.getValue().forEach(i -> System.out.println(i));
           }

        }catch (Exception e)
        {}
    }

}

