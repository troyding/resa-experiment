package storm.resa.simulate.tawc;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import storm.resa.simulate.ExecuteMetric;

import java.util.Map;
import java.util.Random;
import java.util.function.LongSupplier;

/**
 * Created by ding on 14-1-27.
 */
public abstract class TASleepBolt extends BaseRichBolt {

    protected transient OutputCollector collector;
    private LongSupplier sleep;

    public TASleepBolt(LongSupplier sleep) {
        this.sleep = sleep;
    }

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        long inter = this.sleep.getAsLong();
        if (inter > 0) {
            Utils.sleep(inter);
        }
    }
}
