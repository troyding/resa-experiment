package storm.resa.migrate;

import org.codehaus.jackson.map.ObjectMapper;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by ding on 14-5-29.
 */
public class ExecutionAnalyzer {

    public static class ExecutionStat {
        private long count;
        private double cost;

        void add(long count, double cost) {
            this.count += count;
            this.cost += cost;
        }

        void add(ExecutionStat stat) {
            this.count += stat.count;
            this.cost += stat.cost;
        }

        public long getCount() {
            return count;
        }

        public double getCost() {
            return cost;
        }

        @Override
        public String toString() {
            return "ExecutionStat{" + "count=" + count + ", cost=" + cost + '}';
        }
    }

    public ExecutionAnalyzer(Iterable<String> dataStream) {
        this.dataStream = dataStream;
    }

    private Iterable<String> dataStream;
    private ObjectMapper objectMapper = new ObjectMapper();
    private SortedMap<String, ExecutionStat> executeStat = new TreeMap<>();

    public ExecutionAnalyzer calcStat() {
        dataStream.forEach(metricStr -> {
            final String comp;
            final int taskId;
            Map<String, Object> taskData;
            try {
                String[] tmp = metricStr.split("->");
                String[] head = tmp[0].split(":");
                comp = head[0];
                taskId = Integer.parseInt(head[1]);
                taskData = (Map<String, Object>) objectMapper.readValue(tmp[1], Map.class);
            } catch (Exception e) {
                return;
            }
            ((Map<String, String>) taskData.getOrDefault("execute", Collections.emptyMap())).forEach((s, exeStr) -> {
                String[] elements = exeStr.split(",");
                int cnt = Integer.valueOf(elements[0]);
                if (cnt > 0) {
                    double val = Double.valueOf(elements[1]);
                    double val_2 = Double.valueOf(elements[2]);
                    executeStat.computeIfAbsent(String.format("%s:%03d", comp, taskId), (k) -> new ExecutionStat())
                            .add(cnt, val);
                }
            });
        });
        return this;
    }

    public SortedMap<String, ExecutionStat> getStat() {
        return executeStat;
    }

}
