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
                        // retrieve completed tuple
                        Map<String, Object> data = paddingMetricsData.remove(e.getKey());
                        if (data == null) {
                            // never arrived here, just for safe
                            continue;
                        }
                        //add complete-lentency to output json
                        data.put("_complete-latency", e.getValue().intValue());
                        // convert data to json string and add it to redis queue
                        queueDatas.add(new QueueData(queueName, JSONValue.toJSONString(data)));
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
        }
        return null;
    }

}
