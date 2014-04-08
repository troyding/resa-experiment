package storm.resa.util;

import backtype.storm.scheduler.TopologyDetails;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 14-4-8.
 */
public class TopologyHelperTest {

    private static Map<String, Object> conf;

    @BeforeClass
    public static void loadConf() {
        conf = new HashMap<>();
        conf.put("nimbus.host", "localhost");
        conf.put("nimbus.thrift.port", 6627);
        conf.put("storm.thrift.transport", "backtype.storm.security.auth.SimpleTransportPlugin");
        assert conf != null;
    }

    @Test
    public void testGetTopologyDetails() throws Exception {
        TopologyDetails details = TopologyHelper.getTopologyDetails("wordcount", conf);
        Assert.assertEquals("wordcount", details.getName());
        System.out.println("Topology name " + details.getName());
        System.out.println("Num workers " + details.getNumWorkers());
        System.out.println("Spouts " + details.getTopology().get_spouts().keySet());
        System.out.println("Bolts " + details.getTopology().get_bolts().keySet());
    }

    @Test
    public void testComponentToTasks() throws Exception {
        System.out.println(TopologyHelper.componentToTasks(TopologyHelper.getTopologyDetails("wordcount", conf)));
    }
}
