package storm.resa.util;

import backtype.storm.generated.*;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.apache.thrift7.TException;
import org.json.simple.JSONValue;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Helper class to get access online topology running details.
 * Created by ding on 14-4-8.
 */
public class TopologyHelper {

    /**
     * Get a running topology's details.
     *
     * @param topoName topology's name
     * @param conf     storm's conf, used to connect nimbus
     * @return null if topology is not exist, otherwise a TopologyDetails instance
     */
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
            return null;
        } finally {
            nimbusClient.close();
        }
        Map<ExecutorDetails, String> exe2Components = topologyInfo.get_executors().stream()
                .collect(Collectors.toMap(e -> toExecutorDetails(e.get_executor_info()), e -> e.get_component_id()));
        return new TopologyDetails(topologyInfo.get_id(), topologyConf, topology, numWorkers, exe2Components);
    }

    /**
     * Get component tasks from TopologyDetails object
     *
     * @param topoDetails
     * @param ignoreSystemComp whether system component should be ignore
     * @return
     */
    public static Map<String, List<Integer>> componentToTasks(TopologyDetails topoDetails, boolean ignoreSystemComp) {
        Stream<Map.Entry<ExecutorDetails, String>> stream = topoDetails.getExecutorToComponent().entrySet().stream();
        if (ignoreSystemComp) {
            stream = stream.filter(e -> !Utils.isSystemId(e.getValue()));
        }
        return stream.collect(
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

    /**
     * Get bolt component tasks from TopologyDetails object
     *
     * @param topoDetails
     * @return
     */
    public static Map<String, List<Integer>> boltComponentToTasks(TopologyDetails topoDetails) {
        Stream<Map.Entry<ExecutorDetails, String>> stream = topoDetails.getExecutorToComponent().entrySet().stream();
        stream = stream.filter(e -> topoDetails.getTopology().get_bolts().containsKey(e.getValue()));

        return stream.collect(
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

    /**
     * Get spout component tasks from TopologyDetails object
     *
     * @param topoDetails
     * @return
     */
    public static Map<String, List<Integer>> spoutComponentToTasks(TopologyDetails topoDetails) {
        Stream<Map.Entry<ExecutorDetails, String>> stream = topoDetails.getExecutorToComponent().entrySet().stream();
        stream = stream.filter(e -> topoDetails.getTopology().get_spouts().containsKey(e.getValue()));

        return stream.collect(
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

    /**
     * Get component tasks from TopologyDetails object
     *
     * @param topoDetails
     * @return
     */
    public static Map<String, List<Integer>> componentToTasks(TopologyDetails topoDetails) {
        return componentToTasks(topoDetails, false);
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
