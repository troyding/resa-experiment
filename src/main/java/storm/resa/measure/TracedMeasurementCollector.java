package storm.resa.measure;

import backtype.storm.generated.StormTopology;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import org.codehaus.jackson.map.ObjectMapper;
import storm.resa.metric.RedisMetricsCollector;

import java.io.IOException;
import java.util.*;

/**
 * Created by ding on 14-1-27.
 */
public class TracedMeasurementCollector extends RedisMetricsCollector {

    // map data: <sid> --->  <component> ---> <metrics>
    private transient Map<String, Map<String, Object>> paddingMetricsData = new HashMap<String, Map<String, Object>>();
    private transient Set<String> spouts = new HashSet<String>();
    private transient TreeMap<Long, Object[]> waitingTuples = new TreeMap<Long, Object[]>();
    private transient ObjectMapper objectMapper = new ObjectMapper();

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

    private void tupleFinished(String tupleId, Long completeLatency, List<QueueElement> output) {
        tupleFinished(tupleId, completeLatency, output, 0);
    }

    private void tupleFinished(String tupleId, Long completeLatency, List<QueueElement> output, int tryCount) {
        // retrieve completed tuple
        Map<String, Object> data = paddingMetricsData.remove(tupleId);
        if (data == null && tryCount < 3) {
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
                    waitingTuples.put(nextCheckTime, new Object[]{tupleId, completeLatency, tryCount + 1});
                    break;
                }
            }
        } else {
            // no other metric was found, just output "_complete-latency"
            if (data == null) {
                data = new HashMap<>();
            }
            //add complete-lentency to output json
            data.put("_complete-latency", completeLatency.intValue());
            // convert data to json string and add it to redis queue
            try {
                output.add(createDefaultQueueElement(objectMapper.writeValueAsString(data)));
            } catch (IOException e) {
            }
        }
    }

    @Override
    protected List<QueueElement> dataPoints2QueueElement(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        // write out metrics only when tuple was completed
        // The data structure of output metrics is map
        if (spouts.contains(taskInfo.srcComponentId)) {
            ArrayList<QueueElement> queueElements = new ArrayList<QueueElement>();
            dataPoints.stream().filter((p) -> p.name.equals("tuple-completed")).forEach((dataPoint) -> {
                Map<String, Long> completedData = (Map<String, Long>) dataPoint.value;
                for (Map.Entry<String, Long> e : completedData.entrySet()) {
                    tupleFinished(e.getKey(), e.getValue(), queueElements);
                }
            });
            return queueElements;
        } else {
            for (DataPoint dataPoint : dataPoints) {
                if (!dataPoint.name.equals(taskInfo.srcComponentId + "-exe")) {
                    continue;
                }
                for (Map.Entry<String, List<String>> e : ((Map<String, List<String>>) dataPoint.value).entrySet()) {
                    Map<String, Object> data = paddingMetricsData.computeIfAbsent(e.getKey(), (k) -> new HashMap<>());
                    // this list records one tuple's all metrics for the specified component
                    // each element's format is <SourceComponent>:<SourceStreamId>,<sleepInterval>,<leaveTime>
                    ((List<String>) data.computeIfAbsent(taskInfo.srcComponentId, (k) -> new ArrayList<>()))
                            .addAll(e.getValue());
                }
            }
            long currTime = System.currentTimeMillis();
            // check waiting list
            if (!waitingTuples.isEmpty() && waitingTuples.firstKey() < currTime) {
                Map<Long, Object[]> candidates = new HashMap<Long, Object[]>(waitingTuples.headMap(currTime));
                ArrayList<QueueElement> queueElements = new ArrayList<QueueElement>(candidates.size());
                for (Map.Entry<Long, Object[]> e : candidates.entrySet()) {
                    waitingTuples.remove(e.getKey());
                    Object[] v = e.getValue();
                    tupleFinished((String) v[0], (Long) v[1], queueElements, (Integer) v[2]);
                }
                return queueElements;
            }
        }
        return null;
    }

}
