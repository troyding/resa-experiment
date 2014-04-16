package storm.resa.analyzer;

import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Tom.fu on 16/4/2014.
 */
public class TopologyStatAnalyzer {

    public static Map<String, Object> getDefaultConf() {
        Map<String, Object> conf = new HashMap<>();

        conf.put("nimbus.host", "192.168.0.31");
        conf.put("nimbus.thrift.port", 6627);
        conf.put("storm.thrift.transport", "backtype.storm.security.auth.SimpleTransportPlugin");

        return conf;
    }

    public static void PrintTopoDetail(TopologyDetails td) {
        System.out.println("Topology name " + td.getName());
        System.out.println("Num workers " + td.getNumWorkers());
        System.out.println("Spouts " + td.getTopology().get_spouts().keySet());
        System.out.println("Bolts " + td.getTopology().get_bolts().keySet());
    }
}
