package storm.resa.metric;

import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import org.apache.log4j.Logger;

import java.util.*;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ZkMetricsCollector extends ConsumerBase {

    private static final Logger LOG = Logger.getLogger(ZkMetricsCollector.class);

    private String topologyId;
    private MetricsStorer storer;
    private Set<String> metricNames = new HashSet<String>();

    @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context,
                        IErrorReporter errorReporter) {
        super.prepare(stormConf, registrationArgument, context, errorReporter);
        this.topologyId = context.getStormId();
        storer = new MetricsStorer(stormConf);
    }

    @Override
    protected void handleSelectedDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        Map<String, Object> newMetircs = new HashMap<String, Object>();
        for (DataPoint dataPoint : dataPoints) {
            if (metricNames.contains(dataPoint.name)) {
                newMetircs.put(dataPoint.name, dataPoint.value);
            }
        }
        if (!newMetircs.isEmpty()) {
            storer.setTaskMetrics(
                    topologyId,
                    taskInfo.srcTaskId,
                    mergeMetrics((Map<String, Object>) storer.getTaskMetrics(topologyId,
                            taskInfo.srcTaskId), newMetircs));
        }
    }

    private Map<String, Object> mergeMetrics(Map<String, Object> oldMetircs,
                                             Map<String, Object> newMetircs) {
        if (oldMetircs != null) {
            oldMetircs.putAll(newMetircs);
            return oldMetircs;
        }
        return newMetircs;
    }

    @Override
    public void cleanup() {
        storer.close();
    }

}
