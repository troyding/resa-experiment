package storm.resa.simulate;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Random;
/**
 * Created by ding on 14-1-27.
 */
public class WordCounterT2 extends SimulateBolt {

    private Random rand;
    private double p;

    public WordCounterT2(double mu, double p) {
        super(mu);
        this.p = p;
        rand = new Random();
    }

    @Override
    public void execute(Tuple tuple) {
        super.execute(tuple);
        String sid = tuple.getString(0);
        String word = tuple.getString(1);        
        
        double prob = rand.nextDouble();
        ///double prob = super.GetRander().nextDouble();
        if (prob < this.p){
        	///collector.emit(tuple, new Values(sid, tokenizer.nextToken()));
        	collector.emit("LoopBack", tuple, new Values(sid, word));
        }
        else{
        	collector.emit("Leave", new Values(word + "!!"));
        }        
        ///collector.emit(new Values(word + "!!"));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        ///declarer.declare(new Fields("word", "count"));
        ///declarer.declare(new Fields("word!"));
        declarer.declareStream("LoopBack", new Fields("sid", "word"));
        declarer.declareStream("Leave", new Fields("word!"));
    }
}
