package storm.resa.measure;

import backtype.storm.generated.StormTopology;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import org.codehaus.jackson.map.ObjectMapper;
import storm.resa.metric.RedisMetricsCollector;

import java.io.IOException;
import java.util.*;

/**
 * Created by ding on 14-3-20.
 */
public class WinAggMeasurementCollector extends RedisMetricsCollector {

    private static final Set<String> QUEUE_METRIC_NAME = new HashSet<>(Arrays.asList("__sendqueue", "__receive"));

    private Set<String> spouts = new HashSet<String>();
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void prepare(Map stormConf, Object argument, TopologyContext context, IErrorReporter reporter) {
        super.prepare(stormConf, argument, context, reporter);
        StormTopology topology = context.getRawTopology();
        // get all spouts
        spouts.addAll(topology.get_spouts().keySet());
        // add bolt execute metrics
        topology.get_bolts().keySet().forEach(name -> addMetricName(name + "-exe"));
        QUEUE_METRIC_NAME.forEach(name -> addMetricName(name));
        addMetricName("complete-latency");
    }

    private String object2Json(Object o) {
        try {
            return objectMapper.writeValueAsString(o);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void addQueueMetric(DataPoint dataPoint, Map<String, Object> ret) {
        if (!((Map) dataPoint.value).isEmpty()) {
            ret.put(dataPoint.name.substring(2), dataPoint.value);
        }
    }

    @Override
    protected List<QueueElement> dataPoints2QueueElement(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        Map<String, Object> ret = new HashMap<>();
        if (spouts.contains(taskInfo.srcComponentId)) {
            dataPoints.stream().forEach((dataPoint) -> {
                if (dataPoint.name.equals("complete-latency")) {
                    ret.put("complete-latency", dataPoint.value);
                } else if (QUEUE_METRIC_NAME.contains(dataPoint.name)) {
                    addQueueMetric(dataPoint, ret);
                }
            });
        } else {
            dataPoints.stream().forEach((dataPoint) -> {
                if (dataPoint.name.equals(taskInfo.srcComponentId + "-exe")) {
                    ret.put("execute", dataPoint.value);
                } else if (QUEUE_METRIC_NAME.contains(dataPoint.name)) {
                    addQueueMetric(dataPoint, ret);
                }
            });
        }
        String data = taskInfo.srcComponentId + ':' + taskInfo.srcTaskId + "->" + object2Json(ret);
        return Arrays.asList(createDefaultQueueElement(data));
    }
}