package storm.resa.analyzer;

import org.codehaus.jackson.map.ObjectMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ding on 14-3-4.
 */
public class OrgMetricAnalyzer {

    public OrgMetricAnalyzer(Iterable<Object> dataStream) {
        this.dataStream = dataStream;
    }

    protected Iterable<Object> dataStream;
    private ObjectMapper objectMapper = new ObjectMapper();

    private static class AvgCalc {
        private double total = 0;
        private int count = 0;

        void addNumber(double num) {
            total = total + num;
            count++;
        }

        double getAvg() {
            return total / count;
        }
    }


    public void calcAvg() {
        long totalCompleteLatency = 0;
        Map<String, AvgCalc> compAvg = new HashMap<String, AvgCalc>();
        int count = 0;
        for (Object metricStr : dataStream) {
            Map<String, Object> data;
            try {
                data = (Map<String, Object>) objectMapper.readValue((String) metricStr, Map.class);
            } catch (Exception e) {
                continue;
            }
            Number completeLatency = (Number) data.remove("_complete-latency");
            if (completeLatency != null) {
                totalCompleteLatency = totalCompleteLatency + completeLatency.longValue();
            }
            for (Map.Entry<String, Object> e : data.entrySet()) {
                AvgCalc avgCalc = compAvg.get(e.getKey());
                if (avgCalc == null) {
                    avgCalc = new AvgCalc();
                    compAvg.put(e.getKey(), avgCalc);
                }
                for (String element : (List<String>) e.getValue()) {
                    double num = Double.valueOf(element.split(",")[1]);
                    avgCalc.addNumber(num);
                }
            }
            count++;
        }
        System.out.println("complete latency avg:" + (totalCompleteLatency / count));
        for (Map.Entry<String, AvgCalc> e : compAvg.entrySet()) {
            System.out.print(e.getKey() + " avg :");
            System.out.println(e.getValue().getAvg());
        }
    }

}
