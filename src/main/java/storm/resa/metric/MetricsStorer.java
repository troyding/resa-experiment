package storm.resa.metric;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.json.simple.JSONValue;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@SuppressWarnings({"rawtypes", "unchecked"})
public class MetricsStorer {

    private static final Logger LOG = Logger.getLogger(MetricsStorer.class);

    private ZooKeeperFacade zk;
    private String metricsRootZnode = "/storm/metrics";

    private Set<String> avaliableZnode = new HashSet<String>();

    public MetricsStorer(Map conf) {
        zk = new ZooKeeperFacade(buildConnectString(conf),
                ((Number) conf.get("storm.zookeeper.connection.timeout")).intValue());
        initRootZnode();
    }

    private String buildConnectString(Map stormConf) {
        int port = ((Number) stormConf.get("storm.zookeeper.port")).intValue();
        Collection<String> servers = (Collection<String>) stormConf.get("storm.zookeeper.servers");
        StringBuilder connectString = new StringBuilder();
        for (String ser : servers) {
            connectString.append(ser);
            connectString.append(':');
            connectString.append(port);
            connectString.append(",");
        }
        connectString.deleteCharAt(connectString.length() - 1);
        return connectString.toString();
    }

    private void initRootZnode() {
        if (!zk.znodeExist(metricsRootZnode, null)) {
            LOG.info("parent node is not exist, create it!");
            zk.createZNode(metricsRootZnode, new byte[0], CreateMode.PERSISTENT);
        } else {
            LOG.info("parent node exist, continue");
        }
    }

    private String checkZnode(String topologyId, String znode) {
        if (!avaliableZnode.contains(znode)) {
            String topologyMetricsZnode = metricsRootZnode + '/' + topologyId;
            if (!zk.znodeExist(topologyMetricsZnode, null)) {
                zk.createZNode(topologyMetricsZnode, new byte[0], CreateMode.PERSISTENT);
            }
            if (!zk.znodeExist(znode, null)) {
                zk.createZNode(znode, null, CreateMode.PERSISTENT);
            }
            avaliableZnode.add(znode);
        }
        return znode;
    }

    private String getTaskZnode(String topologyId, Integer taskId) {
        return metricsRootZnode + '/' + topologyId + "/task-" + taskId;
    }

    private String getMetricZnode(String topologyId, String metricName) {
        return metricsRootZnode + '/' + topologyId + '/' + metricName;
    }

    public Map<String, Object> getTaskMetrics(String topologyId, int task) {
        String taskZnode = getTaskZnode(topologyId, task);
        checkZnode(topologyId, taskZnode);
        byte[] data = zk.znodeData(taskZnode, null);
        return data == null || data.length == 0 ? null : (Map<String, Object>) JSONValue
                .parse(new String(data));
    }

    public void setTaskMetrics(String topologyId, int task, Map<String, Object> metrics) {
        String taskZnode = getTaskZnode(topologyId, task);
        checkZnode(topologyId, taskZnode);
        zk.setZNodeData(taskZnode, JSONValue.toJSONString(metrics).getBytes());
    }

    public void setMetric(String topologyId, String metricName, byte[] metricData) {
        String metricZnode = getMetricZnode(topologyId, metricName);
        checkZnode(topologyId, metricZnode);
        zk.setZNodeData(metricZnode, metricData);
    }

    public byte[] getMetric(String topologyId, String metricName) {
        String metricZnode = getMetricZnode(topologyId, metricName);
        checkZnode(topologyId, metricZnode);
        return zk.znodeData(metricZnode, null);
    }

    public void close() {
        zk.close();
    }

}
