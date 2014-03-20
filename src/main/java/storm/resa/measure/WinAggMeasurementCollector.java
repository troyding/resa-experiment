package storm.resa.measure;

import backtype.storm.generated.StormTopology;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import org.codehaus.jackson.map.ObjectMapper;
import storm.resa.metric.RedisMetricsCollector;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by ding on 14-3-20.
 */
public class WinAggMeasurementCollector extends RedisMetricsCollector {

    private Set<String> spouts = new HashSet<String>();
    private ObjectMapper objectMapper = new ObjectMapper();

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
    protected List<QueueElement> dataPoints2QueueElement(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        String filterKey, outKey;
        if (spouts.contains(taskInfo.srcComponentId)) {
            filterKey = "tuple-completed";
            outKey = "_complete-latency";
        } else {
            filterKey = taskInfo.srcComponentId + "-exe";
            outKey = taskInfo.srcComponentId;
        }
        return dataPoints.stream().filter((p) -> p.name.equals(filterKey)).map((dataPoint) -> {
            try {
                String json = objectMapper.writeValueAsString(Collections.singletonMap(outKey,
                        dataPoint.value));
                return new QueueElement(queueName, json);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
    }
}
