package storm.resa.app.wc;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.resa.util.ConfigUtil;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.ReducerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Created by ding on 14-4-4.
 */
public class TridentWordCount {

    public static class SplitFun extends BaseFunction {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            StringTokenizer tokenizer = new StringTokenizer(sentence, "\\s+");
            while (tokenizer.hasMoreTokens()) {
                collector.emit(Arrays.asList((Object) tokenizer.nextToken()));
            }
        }

    }

    public static class Count implements ReducerAggregator<Integer> {
        @Override
        public Integer init() {
            return 0;
        }

        @Override
        public Integer reduce(Integer curr, TridentTuple tuple) {
            return curr + 1;
        }
    }

    public static class MyState extends MemoryMapState {

        public MyState(String id) {
            super(id);
        }

        static class Factory implements StateFactory {
            @Override
            public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
                return new MyState("" + partitionIndex);
            }
        }

        @Override
        public List multiUpdate(List keys, List list) {
            System.out.println("multiUpdate called-" + keys + "###" + list);
            return super.multiUpdate(keys, list);
        }

        @Override
        public void multiPut(List keys, List vals) {
            super.multiPut(keys, vals);
            System.out.println("multiPut called-" + vals);
        }

        @Override
        public List multiGet(List keys) {
            System.out.println("multiGet called, keys=" + keys);
            return super.multiGet(keys);
        }

        @Override
        public void beginCommit(Long txid) {
            super.beginCommit(txid);
            System.out.println("beginCommit-" + txid);
        }

        @Override
        public void commit(Long txid) {
            super.commit(txid);
            System.out.println("commit-" + txid);
        }
    }


    public static void main(String[] args) throws Exception {
        Config conf = ConfigUtil.readConfig(new File(args[1]));
        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[1]);
        }

        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 2,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        topology.newStream("wordcount", spout)
                .each(new Fields("sentence"), new SplitFun(), new Fields("word")).parallelismHint(4)
                .groupBy(new Fields("word"))
                .persistentAggregate(new MyState.Factory(), new Count(), new Fields("count")).parallelismHint(1);
        StormSubmitter.submitTopology(args[0], conf, topology.build());
    }

}
