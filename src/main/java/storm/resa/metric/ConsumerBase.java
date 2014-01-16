package storm.resa.metric;

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Created by ding on 13-12-4.
 */
public abstract class ConsumerBase implements IMetricsConsumer {

    private static final Logger LOG = Logger.getLogger(ZkMetricsCollector.class);

    public static final String METRICS_NAME = "storm.resa.metrics.names";

    private Set<String> metricNames = new HashSet<String>();

    @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context,
                        IErrorReporter errorReporter) {
        Collection<String> names = (List<String>) ((Map<String, Object>) registrationArgument)
                .get(METRICS_NAME);
        //names = Arrays.asList(new String[] { "__process-latency", "__execute-latency" }));
        if (names != null) {
            metricNames.addAll(names);
        }
        LOG.info("To collect metrics:" + metricNames);
    }

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        //ignore system component
        if (Utils.isSystemId(taskInfo.srcComponentId)) {
            return;
        }
        ArrayList<DataPoint> selectedPoints = new ArrayList<DataPoint>(dataPoints.size());
        for (DataPoint dataPoint : dataPoints) {
            if (metricNames.contains(dataPoint.name) && !filter(dataPoint)) {
                selectedPoints.add(dataPoint);
            }
        }
        if (!selectedPoints.isEmpty()) {
            handleSelectedDataPoints(taskInfo, selectedPoints);
        }
    }

    /**
     * Check whether this data point should be filtered
     *
     * @param dataPoint
     * @return true if this data point should be removed
     */
    protected boolean filter(DataPoint dataPoint) {
        Object value = dataPoint.value;
        if (value == null) {
            return true;
        } else if (value instanceof Map) {
            return ((Map) value).isEmpty();
        } else if (value instanceof Collection) {
            return ((Collection) value).isEmpty();
        }
        return false;
    }

    protected void handleSelectedDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {

    }
}
