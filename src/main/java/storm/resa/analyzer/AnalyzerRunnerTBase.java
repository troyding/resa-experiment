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

    static Map<String, Queue<ComponentAggResult>> spoutHistory = new HashMap<>();
    static Map<String, Queue<ComponentAggResult>> boltHistory = new HashMap<>();

    public static void main(String[] args) {

        Map<String, Object> conf = Utils.readDefaultConfig();
        conf.putAll(Utils.readStormConfig());

        Config topConf = ConfigUtil.readConfig(new File(args[4]));
        conf.putAll(topConf);

        TopologyDetails td = TopologyHelper.getTopologyDetails(args[2], conf);

        if (td == null){
            System.out.println("Topology: " + args[2] + " does not exist");
            return;
        }

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

        System.out.println("topology.executor.receive.buffer.size: " + recvQLen);
        System.out.println("topology.executor.send.buffer.size: " + sendQLen);

        ///Iterable<Object> dataStream = new JedisResource(args[0], Integer.valueOf(args[1]), args[2]);
        int timeIntervalSec = Integer.valueOf(args[3]);
        long duration = timeIntervalSec * 1000;
        while (true) {
            // run analyze for each time batch
            AggMetricAnalyzer aggAnalyzer = new AggMetricAnalyzer(new JedisResource(args[0], Integer.valueOf(args[1]), args[2]));
            aggAnalyzer.calCMVStat();

            if (!aggAnalyzer.getSpoutResult().isEmpty() || !aggAnalyzer.getBoltResult().isEmpty()) {
                System.out.println("----------- Message reported on " + System.currentTimeMillis() + " -----------------");

                ///AggMetricAnalyzer.printCMVStatShort(aggAnalyzer.getSpoutResult());
                ///AggMetricAnalyzer.printCMVStatShort(aggAnalyzer.getBoltResult());

                try {

                    MornitorTopologyStat(aggAnalyzer, conf, bolt2t, spout2t);
                }catch (Exception e) {
                    e.printStackTrace();
                }

                Utils.sleep(duration);
            }
        }
    }

    public static void MornitorTopologyStat (
            AggMetricAnalyzer aggAnalyzer,
            Map<String, Object> conf,
            Map<String, List<Integer>> bolt2t,
            Map<String, List<Integer>> spout2t) throws Exception {

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
                car.addCAR(e.getValue());
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
                car.addCAR(e.getValue());
            }
        }

        System.out.println("spWinMorSize: " + spoutWinMor.size() + ",spComMorSize: " + spoutCombineMor.size() + ",spUpCnt: " + spoutUpCnt);
        System.out.println("boWinMorSize: " + boltWinMor.size() + ",boComMorSize: " + boltCombineMor.size() + ",boUpCnt: " + boltUpCnt );

        Map<String, Object> para = new HashMap<>();
        int maxSendQSize = ConfigUtil.getInt(conf, "topology.executor.send.buffer.size", 1024);
        int maxRecvQSize = ConfigUtil.getInt(conf, "topology.executor.receive.buffer.size", 1024);

        para.put("QoS", 5000.0);
        para.put("maxSendQSize", maxSendQSize);
        para.put("maxRecvQSize", maxRecvQSize);
        para.put("sendQSizeThresh", 5.0);
        para.put("recvQSizeThreshRatio", 0.8);
        para.put("messageUpdateInterval", 10.0);
        para.put("historySize", 3);

        for (Map.Entry<String, List<Integer>> e : bolt2t.entrySet()) {
            para.put(e.getKey(), e.getValue().size());
        }

        for (Map.Entry<String, List<Integer>> e : spout2t.entrySet()) {
            para.put(e.getKey(), e.getValue().size());
        }

        ConfigUtil.printConfig(para);

        StatReport(spoutCombineMor, boltCombineMor, para);
    }


    public static void StatReport(
            Map<String, ComponentAggResult> spoutResult,
            Map<String, ComponentAggResult> boltResult,
            Map<String, Object> para) {

        ///Temp use, assume only one running topology!
        double targetQoS = ConfigUtil.getDouble(para, "QoS", 5000.0);
        int maxSendQSize = ConfigUtil.getInt(para, "maxSendQSize", 1024);
        int maxRecvQSize = ConfigUtil.getInt(para, "maxRecvQSize", 1024);
        double sendQSizeThresh = ConfigUtil.getDouble(para, "sendQSizeThresh", 5.0);
        double recvQSizeThreshRatio = ConfigUtil.getDouble(para, "recvQSizeThreshRatio", 0.8);
        double recvQSizeThresh = recvQSizeThreshRatio * maxRecvQSize;
        double updInterval = ConfigUtil.getDouble(para, "messageUpdateInterval", 10.0);

        int historySize = ConfigUtil.getInt(para, "historySize", 3);


        for (Map.Entry<String, ComponentAggResult> e : spoutResult.entrySet()) {
            String cid = e.getKey();
            ComponentAggResult car = e.getValue();
            int taskNum = ConfigUtil.getInt(para, cid, 0);

            Queue<ComponentAggResult> his = spoutHistory.get(cid);
            if (his == null) {
                his = new LinkedList<ComponentAggResult>();
                spoutHistory.put(cid, his);
            }

            his.add(car);
            if (his.size() > historySize) {
                his.poll();
            }

            ComponentAggResult hisCar = ComponentAggResult.getSimpleCombinedHistory(his, car.type);

            System.out.println("-------------------------------------------------------------------------------");
            System.out.println("ComName: " + cid + ", type: " + car.getComponentType() + ", #task: " + taskNum);
            System.out.println("processed: " + car.getSimpleCombinedProcessedTuple().toCMVString());
            System.out.println("his_processed: " + hisCar.getSimpleCombinedProcessedTuple().toCMVString());

            double avgComplete = car.getSimpleCombinedProcessedTuple().getAvg();
            boolean satisfyQoS =  avgComplete < targetQoS;

            System.out.println("TarQoS: " + targetQoS + ", AvgComplete: " + avgComplete + ", satisfy: " + satisfyQoS);
            System.out.println("-------------------------------------------------------------------------------");
        }

        for (Map.Entry<String, ComponentAggResult> e : boltResult.entrySet()) {
            String cid = e.getKey();
            ComponentAggResult car = e.getValue();
            int taskNum = ConfigUtil.getInt(para, cid, 0);

            Queue<ComponentAggResult> his = boltHistory.get(cid);
            if (his == null) {
                his = new LinkedList<ComponentAggResult>();
                boltHistory.put(cid, his);
            }

            his.add(car);
            if (his.size() > historySize) {
                his.poll();
            }

            ComponentAggResult hisCar = ComponentAggResult.getSimpleCombinedHistory(his, car.type);

            System.out.println("ComName: " + cid + ", type: " + car.getComponentType()+ ", #task: " + taskNum);
            System.out.println("SendQLen: " + car.sendQueueLen.toCMVString());
            System.out.println("RecvQLen: " + car.recvQueueLen.toCMVString());
            System.out.println("Arrival: " + car.recvArrivalCnt.toCMVString());
            System.out.println("processed: " + car.getSimpleCombinedProcessedTuple().toCMVString());

            System.out.println("hisSendQLen: " + hisCar.sendQueueLen.toCMVString());
            System.out.println("hisRecvQLen: " + hisCar.recvQueueLen.toCMVString());
            System.out.println("hisArrival: " + hisCar.recvArrivalCnt.toCMVString());
            System.out.println("hisprocessed: " + hisCar.getSimpleCombinedProcessedTuple().toCMVString());

            double avgSendQLen = car.sendQueueLen.getAvg();
            double avgRecvQLen = car.recvQueueLen.getAvg();
            double arrivalRate = car.recvArrivalCnt.getAvg() / updInterval;
            double avgServTime = car.getSimpleCombinedProcessedTuple().getAvg();

            double pho = arrivalRate * avgServTime / 1000;
            double lambda = arrivalRate * taskNum;
            double mu = 1000.0 / car.getSimpleCombinedProcessedTuple().getAvg();

            boolean sendQLenNormal = avgSendQLen < sendQSizeThresh;
            boolean recvQlenNormal = avgRecvQLen < recvQSizeThresh;

            System.out.println(String.format("lambda: %.5f, mu: %.5f, pho: %.5f", lambda, mu, pho) + ",SQ: " + sendQLenNormal + ", RQ: " + recvQlenNormal);
            System.out.println("-------------------------------------------------------------------------------");
        }
    }
}
