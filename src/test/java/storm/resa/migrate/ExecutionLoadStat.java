package storm.resa.migrate;

import storm.resa.util.RedisQueueIterable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;

/**
 * Created by ding on 14-6-18.
 */
public class ExecutionLoadStat {

    private long interval;
    private String queueName = "wc-metrics-0617";

    public ExecutionLoadStat(long interval) {
        this.interval = interval;
    }

    public List<ExecutionAnalyzer.ExecutionStat> doStat(String comp) {
        long[] splits = calcSplit();
        System.out.println(Arrays.toString(splits));
        List<ExecutionAnalyzer.ExecutionStat> res = new ArrayList<>();
        for (int i = 0; i < splits.length; i++) {
            long start = i == 0 ? 0 : splits[i - 1];
            res.add(doStat0(start, splits[i], comp));
        }
        return res;
    }

    private ExecutionAnalyzer.ExecutionStat doStat0(long start, long end, String comp) {
        System.out.println("start-" + start + ", end-" + end);
        try (RedisQueueIterable source = new RedisQueueIterable("192.168.0.30", 6379, queueName, start, end - start)) {
            SortedMap<String, ExecutionAnalyzer.ExecutionStat> res = new ExecutionAnalyzer(source).calcStat().getStat()
                    .subMap(comp, comp + ";");
            ExecutionAnalyzer.ExecutionStat stat = new ExecutionAnalyzer.ExecutionStat();
            res.values().forEach(stat::add);
            return stat;
        } catch (IOException e) {
        }
        return null;
    }

    private long[] calcSplit() {
        long split = 0;
        long last = 0;
        List<Long> splits = new ArrayList<>();
        try (RedisQueueIterable source = new RedisQueueIterable("192.168.0.30", 6379, queueName)) {
            for (String s : source) {
                if (s.startsWith("say:71")) {
                    long time = Long.parseLong(s.split("->")[0].split(":")[2]);
                    if (last == 0) {
                        last = time;
                    } else if (time - last > interval) {
                        splits.add(split);
                        last = time;
                    }
                }
                split++;
            }
        } catch (Exception e) {
        }
        return splits.stream().mapToLong(l -> l).toArray();
    }

    public static void main(String[] args) {
        ExecutionLoadStat executionLoadStat = new ExecutionLoadStat(360);
        executionLoadStat.doStat("counter").forEach(e -> System.out.printf("%.2f\n", e.getCost()));
    }

}
