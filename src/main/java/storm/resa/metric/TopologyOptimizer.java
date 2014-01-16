package storm.resa.metric;

import backtype.storm.generated.Nimbus;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.NimbusClient;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 14-1-16.
 */
public class TopologyOptimizer extends ConsumerBase {

    private static final Logger LOG = Logger.getLogger(TopologyOptimizer.class);
    private Nimbus.Client nimbus;
    private String topology;

    @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context,
                        IErrorReporter errorReporter) {
        super.prepare(stormConf, registrationArgument, context, errorReporter);
        nimbus = NimbusClient.getConfiguredClient(stormConf).getClient();
        String stormId = context.getStormId();
        topology = stormId.substring(0, stormId.indexOf('-'));
    }

    @Override
    protected void handleSelectedDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        // do optimize work

        RebalanceOptions options = new RebalanceOptions();
        //set rebalance options if necessary
        options.set_num_workers(2);//set num_workers if num_workers changed
        options.set_num_executors(new HashMap<String, Integer>());//set set_num_executors if set_num_executors changed

        doTopologyRebalance(topology, options);
    }

    private boolean doTopologyRebalance(String topology, RebalanceOptions options) {
        try {
            nimbus.rebalance(topology, options);
        } catch (Exception e) {
            LOG.warn("do reblance failed");
            return false;
        }
        return true;
    }


    @Override
    public void cleanup() {

    }


}
