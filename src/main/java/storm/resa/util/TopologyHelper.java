package storm.resa.util;

import backtype.storm.generated.*;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.utils.NimbusClient;
import org.apache.thrift7.TException;
import org.json.simple.JSONValue;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Helper class to get access online topology running details.
 * Created by ding on 14-4-8.
 */
public class TopologyHelper {

    public static TopologyDetails getTopologyDetails(String topoName, Map<String, Object> conf) {
        NimbusClient nimbusClient = NimbusClient.getConfiguredClient(conf);
        StormTopology topology;
        TopologyInfo topologyInfo;
        Map topologyConf;
        int numWorkers;
        try {
            Nimbus.Client nimbus = nimbusClient.getClient();
            ClusterSummary cluster = nimbus.getClusterInfo();
            Optional<TopologySummary> topo = cluster.get_topologies().stream()
                    .filter(e -> e.get_name().equals(topoName)).findFirst();
            if (!topo.isPresent()) {
                return null;
            }
            String topoId = topo.get().get_id();
            numWorkers = topo.get().get_num_workers();
            topology = nimbus.getUserTopology(topoId);
            topologyInfo = nimbus.getTopologyInfo(topoId);
            topologyConf = (Map) JSONValue.parse(nimbus.getTopologyConf(topoId));
        } catch (NotAliveException | TException e) {
            e.printStackTrace();
            return null;
        } finally {
            nimbusClient.close();
        }
        Map<ExecutorDetails, String> exe2Components = topologyInfo.get_executors().stream()
                .collect(Collectors.toMap(e -> toExecutorDetails(e.get_executor_info()), e -> e.get_component_id()));
        return new TopologyDetails(topologyInfo.get_id(), topologyConf, topology, numWorkers, exe2Components);
    }

    public static Map<String, List<Integer>> componentToTasks(TopologyDetails topologyDetails) {
        return topologyDetails.getExecutorToComponent().entrySet().stream().collect(
                Collectors.groupingBy(entry -> entry.getValue(),
                        Collector.of((Supplier<List<Integer>>) ArrayList::new, (list, entry) -> {
                            list.addAll(getTaskIds(entry.getKey()));
                        }, (list1, list2) -> {
                            list1.addAll(list2);
                            return list1;
                        })
                )
        );
    }

    private static ExecutorDetails toExecutorDetails(ExecutorInfo executorInfo) {
        return new ExecutorDetails(executorInfo.get_task_start(), executorInfo.get_task_end());
    }

    public static List<Integer> getTaskIds(ExecutorDetails executorDetails) {
        int start = executorDetails.getStartTask();
        int end = executorDetails.getEndTask();
        return start == end ? Arrays.asList(start)
                : IntStream.rangeClosed(start, end).boxed().collect(Collectors.toList());
    }


}
