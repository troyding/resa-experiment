package storm.resa.simulate;

import backtype.storm.generated.StormTopology;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import org.json.simple.JSONValue;
import storm.resa.metric.RedisMetricsCollector;

import java.util.*;

/**
 * Created by ding on 14-1-27.
 */
public class MetricsCollector extends RedisMetricsCollector {

    // map data: <sid> --->  <component> ---> <metrics>
    private transient Map<String, Map<String, Object>> paddingMetricsData = new HashMap<String, Map<String, Object>>();
    private transient Set<String> spouts = new HashSet<String>();
    private transient TreeMap<Long, Object[]> waitingTuples = new TreeMap<Long, Object[]>();

    @Override
    public void prepare(Map stormConf, Object argument, TopologyContext context, IErrorReporter reporter) {
        super.prepare(stormConf, argument, context, reporter);
        StormTopology topology = context.getRawTopology();
        // get all spouts
        spouts.addAll(topology.get_spouts().keySet());
        // add bolt execute metrics
        for (String name : topology.get_bolts().keySet()) {
            addMetricName(name + "-exe");
        }
    }

    private void tupleFinished(String tupleId, Long completeLatency, List<QueueData> output) {
        // retrieve completed tuple
        Map<String, Object> data = paddingMetricsData.remove(tupleId);
        if (data == null) {
            // No tuple metrics found, add it to waiting list
            // check it after 1 min
            while (true) {
                Long nextCheckTime = System.currentTimeMillis() + 60000;
                if (waitingTuples.containsKey(nextCheckTime)) {
                    // avoid time key conflict
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                    }
                } else {
                    waitingTuples.put(nextCheckTime, new Object[]{tupleId, completeLatency});
                    break;
                }
            }
        } else {
            //add complete-lentency to output json
            data.put("_complete-latency", completeLatency.intValue());
            // convert data to json string and add it to redis queue
            output.add(new QueueData(queueName, JSONValue.toJSONString(data)));
        }
    }

    @Override
    protected List<QueueData> dataPoints2QueueElement(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        // write out metrics only when tuple was completed
        // The data structure of output metrics is map
        if (spouts.contains(taskInfo.srcComponentId)) {
            ArrayList<QueueData> queueDatas = new ArrayList<QueueData>();
            for (DataPoint dataPoint : dataPoints) {
                if (dataPoint.name.equals("tuple-completed")) {
                    Map<String, Long> completedData = (Map<String, Long>) dataPoint.value;
                    for (Map.Entry<String, Long> e : completedData.entrySet()) {
                        tupleFinished(e.getKey(), e.getValue(), queueDatas);
                    }
                }
            }
            return queueDatas;
        } else {
            for (DataPoint dataPoint : dataPoints) {
                if (!dataPoint.name.equals(taskInfo.srcComponentId + "-exe")) {
                    continue;
                }
                for (Map.Entry<String, List<String>> e : ((Map<String, List<String>>) dataPoint.value).entrySet()) {
                    Map<String, Object> data = paddingMetricsData.get(e.getKey());
                    if (data == null) {
                        data = new HashMap<String, Object>();
                        paddingMetricsData.put(e.getKey(), data);
                    }
                    // this list records one tuple's all metrics for the specified component
                    // each element's format is <SourceComponent>:<SourceStreamId>,<sleepInterval>,<leaveTime>
                    List<String> componentMetrics = (List<String>) data.get(taskInfo.srcComponentId);
                    if (componentMetrics == null) {
                        componentMetrics = new ArrayList<String>();
                        data.put(taskInfo.srcComponentId, componentMetrics);
                    }
                    componentMetrics.addAll(e.getValue());
                }
            }
            // check waiting list
            if (!waitingTuples.isEmpty() && waitingTuples.firstKey() < System.currentTimeMillis()) {
                Collection<Object[]> candidates = waitingTuples.headMap(System.currentTimeMillis()).values();
                ArrayList<QueueData> queueDatas = new ArrayList<QueueData>(candidates.size());
                for (Object[] e : candidates.toArray(new Object[candidates.size()][])) {
                    tupleFinished((String) e[0], (Long) e[1], queueDatas);
                }
                return queueDatas;
            }
        }
        return null;
    }

}
