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

    private static final Logger LOG = Logger.getLogger(ConsumerBase.class);

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

    public void addMetricName(String name) {
        metricNames.add(name);
    }

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        //ignore system component
        if (keepTask(taskInfo)) {
            return;
        }
        ArrayList<DataPoint> selectedPoints = new ArrayList<DataPoint>(dataPoints.size());
        for (DataPoint dataPoint : dataPoints) {
            if (metricNames.contains(dataPoint.name) && keepPoint(taskInfo, dataPoint)) {
                selectedPoints.add(dataPoint);
            }
        }
        if (!selectedPoints.isEmpty()) {
            handleSelectedDataPoints(taskInfo, selectedPoints);
        }
    }

    protected boolean keepTask(TaskInfo taskInfo) {
        //ignore system component
        return Utils.isSystemId(taskInfo.srcComponentId);
    }

    /**
     * Check whether this data point should be filtered
     *
     * @param dataPoint
     * @return false if this data point should be removed
     */
    protected boolean keepPoint(TaskInfo taskInfo, DataPoint dataPoint) {
        Object value = dataPoint.value;
        if (value == null) {
            return false;
        } else if (value instanceof Map) {
            return !((Map) value).isEmpty();
        } else if (value instanceof Collection) {
            return !((Collection) value).isEmpty();
        }
        return true;
    }

    protected abstract void handleSelectedDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints);
}
