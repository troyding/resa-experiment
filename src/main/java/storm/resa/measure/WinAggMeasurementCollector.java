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

    private String object2Json(Object o) {
        try {
            return objectMapper.writeValueAsString(o);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected List<QueueElement> dataPoints2QueueElement(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        if (spouts.contains(taskInfo.srcComponentId)) {
            return dataPoints.stream().filter((p) -> p.name.equals("tuple-completed")).map((dataPoint) ->
                    new QueueElement(queueName, taskInfo.srcComponentId + "->"
                            + object2Json(Collections.singletonMap("_complete-latency", dataPoint.value))))
                    .collect(Collectors.toList());
        } else {
            Map<String, Object> ret = new HashMap<>();
            dataPoints.stream().forEach((dataPoint) -> {
                if (dataPoint.name.equals(taskInfo.srcComponentId + "-exe")) {
                    ret.put("execute", dataPoint.value);
                } else if (dataPoint.name.equals("__sendqueue")) {
                    if (!((Map) dataPoint.value).isEmpty()) {
                        ret.put("send-queue", dataPoint.value);
                    }
                } else if (dataPoint.name.equals("__receive")) {
                    if (!((Map) dataPoint.value).isEmpty()) {
                        ret.put("recv-queue", dataPoint.value);
                    }
                }
            });
            return Arrays.asList(new QueueElement(queueName, taskInfo.srcComponentId + "->" + object2Json(ret)));
        }
    }
}