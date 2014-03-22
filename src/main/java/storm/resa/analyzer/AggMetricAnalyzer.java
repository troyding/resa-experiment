package storm.resa.analyzer;

import org.codehaus.jackson.map.ObjectMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ding on 14-3-4.
 */
public class AggMetricAnalyzer {

    public AggMetricAnalyzer(Iterable<Object> dataStream) {
        this.dataStream = dataStream;
    }

    protected Iterable<Object> dataStream;
    private ObjectMapper objectMapper = new ObjectMapper();
    static String TupleCompleteLatencyString = "_complete-latency";

    private static class AvgCalc {
        private long total = 0;
        private long count = 0;
        private long total_2 = 0;
        private int emptyEntryCnt = 0;

        void emptyEntryInr (){ emptyEntryCnt++; }
        int getEmptyEntryCnt() { return emptyEntryCnt; }

        ///Add one measured valued
        void addOneNumber(int num) {
            total +=  num;
            count++;
            total_2 += (num * num);
        }

        ///Add aggregated measure values in one window
        void addAggWin(int aggCount, long aggVal, long aggVal_2){
            count += aggCount;
            total += aggVal;
            total_2 += aggVal_2;
        }

        long getTotal() {
        	return total;
        }
        long getCount(){
        	return count;        	
        }
        double getAvg() { return count == 0 ? 0.0 : (double)total / (double)count; }
        double getAvg2() { return count == 0 ? 0.0 : (double)total_2 / (double)count; }
        double getVar(){
        	return getAvg2() - getAvg()*getAvg();
        }
        ///Square coefficient of variation (Scv), scv(X) = Var(X) / [E(X)*E(X)];
        double getScv() {return count == 0 ? 0.0 : (getAvg2() / (getAvg()*getAvg()) - 1.0); }
    }

    public void calcAvg() {
        long totalCompleteLatency = 0;
        Map<String, AvgCalc> compAvg = new HashMap<String, AvgCalc>();
        ///int count = 0;
        for (Object metricStr : dataStream) {
            Map<String, Object> data;
            try {
                data = (Map<String, Object>) objectMapper.readValue((String) metricStr, Map.class);
            } catch (Exception e) {
                continue;
            }
            ///Real example
            ///74) "{\"_complete-latency\":\"1488,61373,22324311\"}"
            ///75) "{\"detector\":{\"projection:default\":\"7255,3736,3820\"}}"
            ///76) "{\"projection\":{\"objectSpout:default\":\"1448,1498,1954\"}}"
            ///77) "{\"updater\":{\"detector:default\":\"4860,974,106962\"}}"
            ///78) "{\"_complete-latency\":\"0,0,0\"}"
            for (Map.Entry<String, Object> e : data.entrySet()) {
                if (e.getKey().equals(TupleCompleteLatencyString)){
                    String latency_element = (String)e.getValue();
                    int cnt = Integer.valueOf(latency_element.split(",")[0]);
                    long val = Long.valueOf(latency_element.split(",")[1]);
                    long val_2 = Long.valueOf(latency_element.split(",")[2]);
                    AvgCalc avgCalc = compAvg.get(TupleCompleteLatencyString);
                    if (avgCalc == null) {
                        avgCalc = new AvgCalc();
                        compAvg.put(TupleCompleteLatencyString, avgCalc);
                    }
                    ///ignore empty entry
                    if (cnt > 0){
                        avgCalc.addAggWin(cnt, val, val_2);
                    }else{
                        avgCalc.emptyEntryInr();
                    }
                }else {
                    AvgCalc avgCalc = compAvg.get(e.getKey());
                    if (avgCalc == null) {
                        avgCalc = new AvgCalc();
                        compAvg.put(e.getKey(), avgCalc);
                    }
                    for (Map.Entry<String, Object> innerE : ((Map<String, Object>)e.getValue()).entrySet()) {
                        String streamName = innerE.getKey();
                        String element = (String)innerE.getValue();
                        int cnt = Integer.valueOf(element.split(",")[0]);
                        long val = Long.valueOf(element.split(",")[1]);
                        long val_2 = Long.valueOf(element.split(",")[2]);
                        if (cnt > 0) {
                            avgCalc.addAggWin(cnt, val, val_2);
                        }else{
                            avgCalc.emptyEntryInr();
                        }
                    }
                }
            }
        }
        ///System.out.println("complete latency avg:" + (totalCompleteLatency / count));
        for (Map.Entry<String, AvgCalc> e : compAvg.entrySet()) {
            //System.out.print(e.getKey() + " avg :");
            //System.out.println(e.getValue().getAvg());
            System.out.print(e.getKey()
                    + ", count: " + e.getValue().getCount()
                    + String.format( ", avg: %.5f", e.getValue().getAvg())
                    + String.format( ", var: %.5f", e.getValue().getVar())
                    + String.format( ", scv: %.5f", e.getValue().getScv())
                    + ", empEn: " + e.getValue().getEmptyEntryCnt());
            System.out.println();
        }
    }
}
