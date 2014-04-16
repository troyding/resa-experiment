package storm.resa.analyzer;

import backtype.storm.Config;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.utils.Utils;
import storm.resa.util.ConfigUtil;
import storm.resa.util.TimeBoundedIterable;
import storm.resa.util.TopologyHelper;

import java.io.File;
import java.util.*;

/**
 * Created by ding on 14-4-14.
 */
public class AnalyzerRunnerTBase {

    static Map<String, ComponentAggResult> spoutWinMor = new HashMap<String, ComponentAggResult>();
    static Map<String, ComponentAggResult> boltWinMor = new HashMap<String, ComponentAggResult>();

    static Map<String, ComponentAggResult> spoutCombineMor = new HashMap<String, ComponentAggResult>();
    static Map<String, ComponentAggResult> boltCombineMor = new HashMap<String, ComponentAggResult>();

    public static void main(String[] args) {

        Map<String, Object> conf = Utils.readDefaultConfig();
        conf.putAll(Utils.readStormConfig());

        Config topConf = ConfigUtil.readConfig(new File(args[4]));
        conf.putAll(topConf);

        TopologyDetails td = TopologyHelper.getTopologyDetails(args[2], conf);

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

        int sendQLen = ConfigUtil.getInt(conf, "topology.executor.send.buffer.size", 1024);
        int recvQLen = ConfigUtil.getInt(conf, "topology.executor.receive.buffer.size", 1024);

        System.out.println("topology.receiver.buffer.size: " + conf.get("topology.receiver.buffer.size"));
        System.out.println("topology.executor.receive.buffer.size: " + recvQLen);
        System.out.println("topology.executor.send.buffer.size: " + sendQLen);

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

                try {

                    MornitorTopologyStat(aggAnalyzer, conf, bolt2t, spout2t);
                }catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }
    }

    public static void MornitorTopologyStat (
            AggMetricAnalyzer aggAnalyzer,
            Map<String, Object> conf,
            Map<String, List<Integer>> bolt2t,
            Map<String, List<Integer>> spout2t) throws Exception {

        double winAlpha = 0.0;
        double qFullThreshold = 0.5;
        double rhoBusyRatio = 1.1;

        Set<String> spoutNames = new HashSet<String>();
        for (Map.Entry<String, List<Integer>> e : spout2t.entrySet()) {
            String componentName = e.getKey();
            for (int i : e.getValue()) {
                String cid = componentName + ":" + i;
                spoutNames.add(cid);
            }
        }

        Set<String> boltNames = new HashSet<String>();
        for (Map.Entry<String, List<Integer>> e : bolt2t.entrySet()) {
            String componentName = e.getKey();
            for (int i : e.getValue()) {
                String cid = componentName + ":" + i;
                boltNames.add(cid);
            }
        }

        int spoutUpCnt = 0;
        int boltUpCnt = 0;
        for (Map.Entry<String, ComponentAggResult> e : aggAnalyzer.getSpoutResult().entrySet()) {
            if (spoutNames.contains(e.getKey())) {
                spoutWinMor.put(e.getKey(), e.getValue());
                spoutUpCnt ++;
            }
        }

        for (Map.Entry<String, ComponentAggResult> e : aggAnalyzer.getBoltResult().entrySet()) {
            if (boltNames.contains(e.getKey())) {
                boltWinMor.put(e.getKey(), e.getValue());
                boltUpCnt ++;
            }
        }

        spoutCombineMor.clear();
        for(Map.Entry<String, ComponentAggResult> e : spoutWinMor.entrySet()) {
            if (e != null) {
                String cid = e.getKey();
                String componentName = cid.split(":")[0];

                ComponentAggResult car = spoutCombineMor.get(componentName);
                if (car == null) {
                    car = new ComponentAggResult(ComponentAggResult.ComponentType.spout);
                    spoutCombineMor.put(componentName, car);
                }
                ComponentAggResult.combine(e.getValue(), car);
            }
        }

        boltCombineMor.clear();
        for(Map.Entry<String, ComponentAggResult> e : boltWinMor.entrySet()) {
            if (e != null) {
                String cid = e.getKey();
                String componentName = cid.split(":")[0];

                ComponentAggResult car = boltCombineMor.get(componentName);
                if (car == null) {
                    car = new ComponentAggResult(ComponentAggResult.ComponentType.bolt);
                    boltCombineMor.put(componentName, car);
                }
                ComponentAggResult.combine(e.getValue(), car);
            }
        }

        System.out.println("spWinMorSize: " + spoutWinMor.size() + ",spComMorSize: " + spoutCombineMor.size() + ",spUpCnt: " + spoutUpCnt);
        System.out.println("boWinMorSize: " + boltWinMor.size() + ",boComMorSize: " + boltCombineMor.size() + ",boUpCnt: " + boltUpCnt );

        AggMetricAnalyzer.printCombineCARStat(spoutCombineMor);
        AggMetricAnalyzer.printCombineCARStat(boltCombineMor);
    }
}
