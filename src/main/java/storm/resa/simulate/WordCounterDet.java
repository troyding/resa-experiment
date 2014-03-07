package storm.resa.simulate;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created by ding on 14-1-27.
 */
public class WordCounterDet extends SimulateDetBolt {

    public WordCounterDet(double mu) {
        super(mu);
    }

    @Override
    public void execute(Tuple tuple) {
        super.execute(tuple);
        String word = tuple.getString(1);
        collector.emit(new Values(word + "!!"));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        ///declarer.declare(new Fields("word", "count"));
        declarer.declare(new Fields("word!"));
    }
}
