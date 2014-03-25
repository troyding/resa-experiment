package storm.resa.simulate.outdet;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import storm.resa.app.cod.Detector;
import storm.resa.app.cod.ObjectSpout;
import storm.resa.measure.AggExecuteMetric;

import java.util.*;

/**
 * Created by ding on 14-3-14.
 */
public class testUpdater implements IRichBolt {

    private OutputCollector collector;
    private Map<String, List<BitSet>> padding;
    private int projectionSize;
    private transient AggExecuteMetric executeMetric;

    public testUpdater(int projectionSize) {
        this.projectionSize = projectionSize;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        padding = new HashMap<>();
        executeMetric = context.registerMetric(context.getThisComponentId() + "-exe", new AggExecuteMetric(), 10);
    }

    @Override
    public void execute(Tuple input) {

        String id = input.getSourceComponent() + ":" + input.getSourceStreamId();
        long arrivalTime = System.currentTimeMillis();

        String key = input.getValueByField(ObjectSpout.TIME_FILED) + "-" + input.getValueByField(ObjectSpout.ID_FILED);
        List<BitSet> ret = padding.get(key);
        if (ret == null) {
            ret = new ArrayList<>();
            padding.put(key, ret);
        }
        ret.add((BitSet) input.getValueByField(Detector.OUTLIER_FIELD));
        if (ret.size() == projectionSize) {
            padding.remove(key);
            BitSet result = ret.get(0);
            ret.stream().forEach((bitSet) -> {
                if (result != bitSet) {
                    result.or(bitSet);
                }
            });
            // output
            //System.out.println(result);
//            result.stream().forEach((status) -> {
//                if (status == 0) {
//                    // output
//                    collector.emit(new Values());
//                }
//            });
        }
        collector.ack(input);
        executeMetric.addMetric(id, (int) (System.currentTimeMillis() - arrivalTime));
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
