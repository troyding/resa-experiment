package storm.resa.analyzer;

import backtype.storm.Config;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.utils.Utils;
import storm.resa.util.ConfigUtil;
import storm.resa.util.TopologyHelper;

import java.io.File;
import java.util.HashMap;
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
           conf.putAll(Utils.readStormConfig());

           Config topConf = ConfigUtil.readConfig(new File(args[4]));
           conf.putAll(topConf);

           TopologyDetails td = TopologyHelper.getTopologyDetails(args[3], conf);
           PrintTopologyDetail(td);

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

           double winAlpha = 0.0;
           double qFullThreshold = 0.5;

           int sendQLen = ConfigUtil.getInt(conf, "topology.executor.send.buffer.size", 1024);
           int recvQLen = ConfigUtil.getInt(conf, "topology.executor.receive.buffer.size", 1024);

           System.out.println("topology.receiver.buffer.size: " + conf.get("topology.receiver.buffer.size"));
           System.out.println("topology.executor.receive.buffer.size: " + recvQLen);
           System.out.println("topology.executor.send.buffer.size: " + sendQLen);

        }catch (Exception e)
        {}
    }

    public static void PrintTopologyDetail(TopologyDetails td) {
        System.out.println("Topology name " + td.getName());
        System.out.println("Num workers " + td.getNumWorkers());
        System.out.println("Spouts " + td.getTopology().get_spouts().keySet());
        System.out.println("Bolts " + td.getTopology().get_bolts().keySet());
    }

    public static void MornitorTopologyStat(
            AggMetricAnalyzer aggAnalyzer,
            Map<String, Object> conf,
            Map<String, List<Integer>> bolt2t,
            Map<String, List<Integer>> spout2t) {
    }

}

