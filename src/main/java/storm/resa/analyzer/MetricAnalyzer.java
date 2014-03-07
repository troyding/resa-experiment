package storm.resa.analyzer;

import org.codehaus.jackson.map.ObjectMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ding on 14-3-4.
 */
public class MetricAnalyzer {

    public MetricAnalyzer(Iterable<Object> dataStream) {
        this.dataStream = dataStream;
    }

    protected Iterable<Object> dataStream;
    private ObjectMapper objectMapper = new ObjectMapper();

    private static class AvgCalc {
        private double total = 0;
        private int count = 0;

        private double total_2 = 0;
        
        void addNumber(double num) {
            total = total + num;            
            count++;
            
            total_2 = total_2 + num*num;
        }

///        double getAvg() {
///            return total / count;
///        }
   
        double getTotal() {
        	return total;
        }
        
        int getCount(){
        	return count;        	
        }
        
        double getAvg() {
        	if (count == 0){
        		return 0.0;
        	}
            return total / (double)count;
        }
        
        double getAvg2() {
        	if (count == 0){
        		return 0.0;
        	}
            return total_2 / (double)count;
        }
        
        double getVar(){
        	return getAvg2() - getAvg()*getAvg();
        }        
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
            Number completeLatency = (Number) data.remove("_complete-latency");
            if (completeLatency != null) {
                ///totalCompleteLatency = totalCompleteLatency + completeLatency.longValue();
            	AvgCalc avgCalc = compAvg.get("_complete-latency");
            	 if (avgCalc == null) {
                     avgCalc = new AvgCalc();
                     compAvg.put("_complete-latency", avgCalc);
                 }
            	 avgCalc.addNumber(completeLatency.doubleValue());
            }
            for (Map.Entry<String, Object> e : data.entrySet()) {
                AvgCalc avgCalc = compAvg.get(e.getKey());
                if (avgCalc == null) {
                    avgCalc = new AvgCalc();
                    compAvg.put(e.getKey(), avgCalc);
                }
                for (String element : (List<String>) e.getValue()) {
                    double num = Double.valueOf(element.split(",")[2]);
                    avgCalc.addNumber(num);
                }

                AvgCalc avgCalc2 = compAvg.get(e.getKey() + "_diff");
                if (avgCalc2 == null) {
                    avgCalc2 = new AvgCalc();
                    compAvg.put(e.getKey() + "_diff", avgCalc2);
                }
                for (String element : (List<String>) e.getValue()) {
                    long arrival = Long.valueOf(element.split(",")[1]);
                    long depature = Long.valueOf(element.split(",")[3]);
                    double dur = (double)(depature - arrival);
                    avgCalc2.addNumber(dur);
                }
            }
            ///count++;
        }
        ///System.out.println("complete latency avg:" + (totalCompleteLatency / count));
        for (Map.Entry<String, AvgCalc> e : compAvg.entrySet()) {
            //System.out.print(e.getKey() + " avg :");
            //System.out.println(e.getValue().getAvg());
            System.out.print(e.getKey() 
            		+ ", count :" + e.getValue().getCount() 
            		+ ", total: " + e.getValue().getTotal()
            		+ ", avg: " + e.getValue().getAvg()
            		+ ", var: " + e.getValue().getVar());
            System.out.println();
        }
    }

}
