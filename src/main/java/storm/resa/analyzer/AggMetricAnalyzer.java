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
    static String TupleExecString = "execute";

    private static class CntMeanVar {
        private long count = 0;
        private double total = 0;
        private double total_2 = 0;
        private int emptyEntryCnt = 0;

        void emptyEntryInr (){ emptyEntryCnt++; }
        int getEmptyEntryCnt() { return emptyEntryCnt; }

        ///Add one measured valued
        void addOneNumber(double num) {
            count++;
            total +=  num;
            total_2 += (num * num);
        }

        ///Add aggregated measure values in one window
        void addAggWin(int aggCount, double aggVal, double aggVal_2){
            count += aggCount;
            total += aggVal;
            total_2 += aggVal_2;
        }

        long getCount(){
            return count;
        }
        double getTotal() {
        	return total;
        }
        double getAvg() { return count == 0 ? 0.0 : total / (double)count; }
        double getAvg2() { return count == 0 ? 0.0 : total_2 / (double)count; }
        double getVar(){
        	return getAvg2() - getAvg()*getAvg();
        }
        ///Square coefficient of variation (Scv), scv(X) = Var(X) / [E(X)*E(X)];
        double getScv() {return count == 0 ? 0.0 : (getAvg2() / (getAvg()*getAvg()) - 1.0); }
    }

    public void calStat() {
        Map<String, CntMeanVar> compAvg = new HashMap<String, CntMeanVar>();
        for (Object metricStr : dataStream) {
            ///Real example
            ///projection->{\"recv-queue\":{\"sampleCount\":239,\"totalQueueLen\":179848,\"totalCount\":4772},
            ///\"send-queue\":{\"sampleCount\":6595,\"totalQueueLen\":1451664,\"totalCount\":131914},
            // \"execute\":{\"objectSpout:default\":\"5996,11552,2708238\"}}
            // 1) "projection->{\"execute\":{\"objectSpout:default\":\"1500,6277.574138,219375.31620289956\"}}"
            //3) "objectSpout->{\"_complete-latency\":\"1724,10500070,67361108512\"}"
            //6) "detector->{\"execute\":{\"projection:default\":\"183245,15408.469740999753,308548.6086445808\"}}"
            //8) "updater->{\"execute\":{\"detector:default\":\"8310,350.70726800000006,136.56612779089428\"}}"

            String componentName = null;
            Map<String, Object> componentData = null;
            try {
                componentName = ((String) metricStr).split("->")[0];
                String componentDataStr = ((String) metricStr).split("->")[1];
                componentData = (Map<String, Object>) objectMapper.readValue(componentDataStr, Map.class);

            } catch (Exception e) {
                continue;
            }

            for (Map.Entry<String, Object> e : componentData.entrySet()) {
                if (e.getKey().equals(TupleCompleteLatencyString)) {
                    String latency_element = (String) e.getValue();
                    int cnt = Integer.valueOf(latency_element.split(",")[0]);
                    double val = Double.valueOf(latency_element.split(",")[1]);
                    double val_2 = Double.valueOf(latency_element.split(",")[2]);
                    String itemNameString = componentName + "->" + e.getKey();
                    CntMeanVar cmv = compAvg.get(itemNameString);
                    if (cmv == null) {
                        cmv = new CntMeanVar();
                        compAvg.put(itemNameString, cmv);
                    }
                    ///ignore empty entry
                    if (cnt > 0) {
                        cmv.addAggWin(cnt, val, val_2);
                    } else {
                        cmv.emptyEntryInr();
                    }
                } else if (e.getKey().equals(TupleExecString)){
                    for (Map.Entry<String, Object> innerE : ((Map<String, Object>)e.getValue()).entrySet()) {
                        String streamName = innerE.getKey();
                        String element = (String)innerE.getValue();
                        int cnt = Integer.valueOf(element.split(",")[0]);
                        double val = Double.valueOf(element.split(",")[1]);
                        double val_2 = Double.valueOf(element.split(",")[2]);
                        String itemNameString = componentName + "->" + streamName + "->" + e.getKey();
                        CntMeanVar cmv = compAvg.get(itemNameString);
                        if (cmv == null) {
                            cmv = new CntMeanVar();
                            compAvg.put(itemNameString, cmv);
                        }
                        if (cnt > 0) {
                            cmv.addAggWin(cnt, val, val_2);
                        }else{
                            cmv.emptyEntryInr();
                        }
                    }
                }else if (e.getKey().contains("queue")){
                    long sampleCnt = 0;
                    long totalQLen =0;
                    long totalArrivalCnt = 0;

                    for (Map.Entry<String, Object> innerE : ((Map<String, Object>)e.getValue()).entrySet()) {
                        String statName = innerE.getKey();
                        int element = (int)(innerE.getValue());

                        if (statName.equals("sampleCount")){
                            sampleCnt = (long)element;
                        } else if (statName.equals("totalQueueLen")) {
                            totalQLen = (long)element;
                        } else if (statName.equals("totalCount")){
                            totalArrivalCnt = (long)element;
                        }
                    }

                    String itemNameString = componentName + "->" + e.getKey() + "->" + "sampleCount";
                    CntMeanVar cmv = compAvg.get(itemNameString);
                    if (cmv == null) {
                        cmv = new CntMeanVar();
                        compAvg.put(itemNameString, cmv);
                    }
                    if (sampleCnt > 0) {
                        cmv.addOneNumber((double)sampleCnt);
                    }else{
                        cmv.emptyEntryInr();
                    }

                    itemNameString = componentName + "->" + e.getKey() + "->" + "QueueLen";
                    cmv = compAvg.get(itemNameString);
                    if (cmv == null) {
                        cmv = new CntMeanVar();
                        compAvg.put(itemNameString, cmv);
                    }
                    if (sampleCnt > 0) {
                        double avgQLen = (double)totalQLen / (double)sampleCnt;
                        cmv.addOneNumber(avgQLen);
                    }else{
                        cmv.emptyEntryInr();
                    }

                    itemNameString = componentName + "->" + e.getKey() + "->" + "ArrivalCnt";
                    cmv = compAvg.get(itemNameString);
                    if (cmv == null) {
                        cmv = new CntMeanVar();
                        compAvg.put(itemNameString, cmv);
                    }
                    if (totalArrivalCnt > 0) {
                        cmv.addOneNumber((double)totalArrivalCnt);
                    }else{
                        cmv.emptyEntryInr();
                    }
                }
            }
        }
        ///System.out.println("complete latency avg:" + (totalCompleteLatency / count));
        for (Map.Entry<String, CntMeanVar> e : compAvg.entrySet()) {
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
