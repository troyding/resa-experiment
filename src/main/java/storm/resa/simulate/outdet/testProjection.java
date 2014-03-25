package storm.resa.simulate.outdet;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.resa.app.cod.ObjectSpout;
import storm.resa.measure.AggExecuteMetric;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Created by ding on 14-3-14.
 */
public class testProjection implements IRichBolt {

    public static final String PROJECTION_ID_FIELD = "projectionId";
    public static final String PROJECTION_VALUE_FIELD = "projectionValue";

    private List<double[]> randomVectors;
    private transient OutputCollector collector;
    private transient AggExecuteMetric executeMetric;

    public testProjection(List<double[]> randomVectors) {
        this.randomVectors = randomVectors;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        executeMetric = context.registerMetric(context.getThisComponentId() + "-exe", new AggExecuteMetric(), 10);
    }

    @Override
    public void execute(Tuple input) {
        String id = input.getSourceComponent() + ":" + input.getSourceStreamId();
        long arrivalTime = System.currentTimeMillis();

        Object objId = input.getValueByField(ObjectSpout.ID_FILED);
        Object time = input.getValueByField(ObjectSpout.TIME_FILED);
        double[] v = (double[]) input.getValueByField(ObjectSpout.VECTOR_FILED);
        double[] val = new double[randomVectors.size()];

        IntStream.range(0, randomVectors.size()).forEach((i) -> {
            val[i] = innerProduct(randomVectors.get(i), v);
        });

        executeMetric.addMetric(id, (int) (System.currentTimeMillis() - arrivalTime));

        IntStream.range(0, randomVectors.size()).forEach((i) -> {
            collector.emit(input, new Values(objId, i, val[i], time));
        });
        collector.ack(input);
    }

    private static double innerProduct(double[] v1, double[] v2) {
        if (v1.length != v2.length) {
            throw new IllegalArgumentException();
        }
        return IntStream.range(0, v1.length).mapToDouble((i) -> v1[i] * v2[i]).sum();
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(ObjectSpout.ID_FILED, PROJECTION_ID_FIELD,
                PROJECTION_VALUE_FIELD, ObjectSpout.TIME_FILED));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
