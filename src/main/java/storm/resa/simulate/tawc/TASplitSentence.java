package storm.resa.simulate.tawc;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.resa.simulate.SimulateBolt;

import java.util.StringTokenizer;
import java.util.function.LongSupplier;

/**
 * Created by ding on 14-1-27.
 */
public class TASplitSentence extends TASleepBolt {
    private static final long serialVersionUID = 9182719848878455933L;

    public TASplitSentence(LongSupplier sleep) {
        super(sleep);
    }

    public void execute(Tuple tuple) {
        super.execute(tuple);
        String sid = tuple.getString(0);
        String sentence = tuple.getString(1);
        StringTokenizer tokenizer = new StringTokenizer(sentence);
        while (tokenizer.hasMoreTokens()) {
            collector.emit(tuple, new Values(sid, tokenizer.nextToken()));
        }
        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sid", "word"));
    }
}