package storm.resa.measure;

import backtype.storm.generated.StormTopology;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import org.codehaus.jackson.map.ObjectMapper;
import storm.resa.metric.RedisMetricsCollector;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by ding on 14-3-20.
 */
public class WinAggMeasurementCollector extends RedisMetricsCollector {

    private static final Map<String, String> METRICS_NAME_MAPPING = new HashMap<>();

    static {
        // add metric name mapping here
        METRICS_NAME_MAPPING.put("__sendqueue", "send-queue");
        METRICS_NAME_MAPPING.put("__receive", "recv-queue");
        METRICS_NAME_MAPPING.put("complete-latency", "complete-latency");
        METRICS_NAME_MAPPING.put("execute", "execute");
    }

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void prepare(Map stormConf, Object argument, TopologyContext context, IErrorReporter reporter) {
        super.prepare(stormConf, argument, context, reporter);
        StormTopology topology = context.getRawTopology();
        // register all needed metric
        METRICS_NAME_MAPPING.keySet().forEach(name -> addMetricName(name));
    }

    private String object2Json(Object o) {
        try {
            return objectMapper.writeValueAsString(o);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected List<QueueElement> dataPoints2QueueElement(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        Map<String, Object> ret = dataPoints.stream().collect(
                Collectors.toMap(p -> METRICS_NAME_MAPPING.get(p.name), p -> p.value));
        String data = taskInfo.srcComponentId + ':' + taskInfo.srcTaskId + "->" + object2Json(ret);
        return Arrays.asList(createDefaultQueueElement(data));
    }
}