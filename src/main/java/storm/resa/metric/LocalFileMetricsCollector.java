package storm.resa.metric;

import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import org.apache.commons.io.IOUtils;
import org.json.simple.JSONValue;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 13-12-4.
 */
public class LocalFileMetricsCollector extends ConsumerBase {

    private BufferedWriter writer;

    @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context,
                        IErrorReporter errorReporter) {
        super.prepare(stormConf, registrationArgument, context, errorReporter);
        String outputFileName = (String) ((Map<String, Object>) registrationArgument).get("storm.resa.metrics.local-file");
        // default write data to metric.log located in current dir
        if (outputFileName == null) {
            outputFileName = "metric.log";
        }
        try {
            writer = new BufferedWriter(new FileWriter(outputFileName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void handleSelectedDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        Map<String, Object> metrics = new HashMap<String, Object>((int) (dataPoints.size() / 0.75f) + 1, 0.75f);
        for (DataPoint dataPoint : dataPoints) {
            metrics.put(dataPoint.name, dataPoint.value);
        }
        String s = '[' + taskInfo.srcComponentId + '-' + taskInfo.srcTaskId + "]:" +
                JSONValue.toJSONString(metrics) + '\n';
        try {
            writer.write(s);
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void cleanup() {
        IOUtils.closeQuietly(writer);
    }
}
