package storm.resa.app.fp;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by ding on 14-6-5.
 */
public class PatternReporter extends BaseRichBolt implements Constant {

    private OutputCollector collector;
    private Map<Integer, String> invdict;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        int id = 0;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(this.getClass().getResourceAsStream("/dict.txt")))) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                invdict.put(id++, line);
                ///dict.put(line, id++);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple input) {
        WordList wordList = (WordList) input.getValueByField(PATTERN_FIELD);
        List<String> words = IntStream.of(wordList.getWords()).mapToObj(invdict::get).collect(Collectors.toList());
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(PATTERN_FIELD, IS_ADD_FIELD));
    }
}
