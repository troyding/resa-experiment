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
    static String TupleCompleteLatencyString = "complete-latency";
    static String TupleExecString = "execute";
    static String TupleSendQueueString = "send-queue";
    static String TupleRecvQueueString = "recv-queue";

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

        String toCMVString(){
            return  "Count: " + getCount()
                    + String.format( ", avg: %.5f", getAvg())
                    + String.format( ", var: %.5f", getVar())
                    + String.format( ", scv: %.5f", getScv())
                    + ", empEn: " + getEmptyEntryCnt();
        }
    }

    private static class ComponentAggResult{
        CntMeanVar recvArrivalCnt = null;
        CntMeanVar recvQueueLen = null;
        CntMeanVar recvQueueSampleCnt = null;

        CntMeanVar sendArrivalCnt = null;
        CntMeanVar sendQueueLen = null;
        CntMeanVar sendQueueSampleCnt = null;

        Map<String, CntMeanVar> tupleProcess = null;
        enum ComponentType {bolt, spout};
        ComponentType type = null;

        ComponentAggResult(ComponentType t){
            this.type = t;
            recvArrivalCnt = new CntMeanVar();
            recvQueueLen = new CntMeanVar();
            recvQueueSampleCnt = new CntMeanVar();

            sendArrivalCnt = new CntMeanVar();
            sendQueueLen = new CntMeanVar();
            sendQueueSampleCnt = new CntMeanVar();

            tupleProcess = new HashMap<>();
        }

        String getComponentType(){
            return type == ComponentType.bolt ? "bolt":"spout";
        }

        String getProcessString(){
            return type == ComponentType.bolt ? "exec-delay":"complete-latency";
        }

    }

    public void calCMVStat() {
        Map<String, CntMeanVar> compAvg = new HashMap<String, CntMeanVar>();

        Map<String[], ComponentAggResult> spoutResult = new HashMap<String[], ComponentAggResult>();
        Map<String[], ComponentAggResult> boltResult = new HashMap<String[], ComponentAggResult>();

        for (Object metricStr : dataStream) {
            ///Real example
            //69) "objectSpout:4->{\"receive\":{\"sampleCount\":209,\"totalQueueLen\":212,\"totalCount\":4170},\"complete-latency\":{\"default\":\"2086,60635.0,3382707.0\"},\"sendqueue\":{\"sampleCount\":420,\"totalQueueLen\":424,\"totalCount\":8402}}"
            //70) "projection:7->{\"receive\":{\"sampleCount\":52,\"totalQueueLen\":53,\"totalCount\":1052},\"sendqueue\":{\"sampleCount\":2152,\"totalQueueLen\":4514,\"totalCount\":43052},\"execute\":{\"objectSpout:default\":\"525,709.4337659999997,1120.8007487084597\"}}"
            //71) "detector:3->{\"receive\":{\"sampleCount\":2769,\"totalQueueLen\":6088758,\"totalCount\":55416},\"sendqueue\":{\"sampleCount\":8921,\"totalQueueLen\":11476,\"totalCount\":178402},\"execute\":{\"projection:default\":\"49200,5167.623237000047,721.6383647758853\"}}"
            //73) "updater:9->{\"receive\":{\"sampleCount\":3921,\"totalQueueLen\":5495,\"totalCount\":78436},\"sendqueue\":{\"sampleCount\":4001,\"totalQueueLen\":4336,\"totalCount\":80002},\"execute\":{\"detector:default\":\"40000,1651.7782049999894,182.68124734051045\"}}"
            String componentFullName = null;
            Map<String, Object> componentData = null;
            try {
                componentFullName = ((String) metricStr).split("->")[0];
                String componentDataStr = ((String) metricStr).split("->")[1];
                componentData = (Map<String, Object>) objectMapper.readValue(componentDataStr, Map.class);

            } catch (Exception e) {
                continue;
            }

            if (componentData.containsKey(TupleCompleteLatencyString) || componentData.containsKey(TupleExecString)) {///Spout case
                String componentName = componentFullName.split(":")[0];
                String taskID = componentFullName.split(":")[1];

                String[] cid = new String[]{componentName, taskID};
                ComponentAggResult car = null;
                if (componentData.containsKey(TupleCompleteLatencyString)) {
                    car = spoutResult.get(cid);
                    if (car == null) {
                        car = new ComponentAggResult(ComponentAggResult.ComponentType.spout);
                        spoutResult.put(cid, car);
                    }
                }else if (componentData.containsKey(TupleExecString)){
                    car = boltResult.get(cid);
                    if (car == null) {
                        car = new ComponentAggResult(ComponentAggResult.ComponentType.bolt);
                        boltResult.put(cid, car);
                    }
                }

                for (Map.Entry<String, Object> e : componentData.entrySet()) {
                    if (e.getKey().equals(TupleCompleteLatencyString) || e.getKey().equals(TupleExecString)) {
                        for (Map.Entry<String, Object> innerE : ((Map<String, Object>) e.getValue()).entrySet()) {
                            String streamName = innerE.getKey();
                            String element = (String) innerE.getValue();
                            int cnt = Integer.valueOf(element.split(",")[0]);
                            double val = Double.valueOf(element.split(",")[1]);
                            double val_2 = Double.valueOf(element.split(",")[2]);

                            CntMeanVar cmv = car.tupleProcess.get(streamName);
                            if (cmv == null) {
                                cmv = new CntMeanVar();
                                car.tupleProcess.put(streamName, cmv);
                            }
                            if (cnt > 0) {
                                cmv.addAggWin(cnt, val, val_2);
                            } else {
                                cmv.emptyEntryInr();
                            }
                        }
                    } else if (e.getKey().equals(TupleSendQueueString)) {///send queue
                        long sampleCnt = 0;
                        long totalQLen = 0;
                        long totalArrivalCnt = 0;

                        for (Map.Entry<String, Object> innerE : ((Map<String, Object>) e.getValue()).entrySet()) {
                            String statName = innerE.getKey();
                            int element = (int) (innerE.getValue());

                            if (statName.equals("sampleCount")) {
                                sampleCnt = (long) element;
                            } else if (statName.equals("totalQueueLen")) {
                                totalQLen = (long) element;
                            } else if (statName.equals("totalCount")) {
                                totalArrivalCnt = (long) element;
                            }
                        }
                        if (sampleCnt > 0) {
                            car.sendQueueSampleCnt.addOneNumber((double) sampleCnt);
                            double avgQLen = (double) totalQLen / (double) sampleCnt;
                            car.sendQueueLen.addOneNumber(avgQLen);
                        } else {
                            car.sendQueueSampleCnt.emptyEntryInr();
                            car.sendQueueLen.emptyEntryInr();
                        }
                        if (totalArrivalCnt > 0) {
                            car.sendArrivalCnt.addOneNumber((double) totalArrivalCnt);
                        } else {
                            car.sendArrivalCnt.emptyEntryInr();
                        }
                    } else if (e.getKey().equals(TupleRecvQueueString)) {///receive queue
                        long sampleCnt = 0;
                        long totalQLen = 0;
                        long totalArrivalCnt = 0;

                        for (Map.Entry<String, Object> innerE : ((Map<String, Object>) e.getValue()).entrySet()) {
                            String statName = innerE.getKey();
                            int element = (int) (innerE.getValue());

                            if (statName.equals("sampleCount")) {
                                sampleCnt = (long) element;
                            } else if (statName.equals("totalQueueLen")) {
                                totalQLen = (long) element;
                            } else if (statName.equals("totalCount")) {
                                totalArrivalCnt = (long) element;
                            }
                        }
                        if (sampleCnt > 0) {
                            car.recvQueueSampleCnt.addOneNumber((double) sampleCnt);
                            double avgQLen = (double) totalQLen / (double) sampleCnt;
                            car.recvQueueLen.addOneNumber(avgQLen);
                        } else {
                            car.recvQueueSampleCnt.emptyEntryInr();
                            car.recvQueueLen.emptyEntryInr();
                        }
                        if (totalArrivalCnt > 0) {
                            car.recvArrivalCnt.addOneNumber((double) totalArrivalCnt);
                        } else {
                            car.recvArrivalCnt.emptyEntryInr();
                        }
                    }
                }
            }
        }
        ///System.out.println("complete latency avg:" + (totalCompleteLatency / count));

        for (Map.Entry<String[], ComponentAggResult> e: spoutResult.entrySet()){
            String[] cid = e.getKey();
            ComponentAggResult car = e.getValue();
            int tupleProcessCnt = car.tupleProcess.size();

            System.out.println("ComponentName: " + cid[0] + ", taskID: " + cid[1] + ", type: " + car.getComponentType() + ", tupleProcessCnt: " + tupleProcessCnt);
            System.out.println("----------------------------- SEND QUEUE Detail ----------------------------------------------");
            System.out.println("SendQueue->SampleCnt: " + car.sendQueueSampleCnt.toCMVString());
            System.out.println("SendQueue->QueueLen: " + car.sendQueueLen.toCMVString());
            System.out.println("SendQueue->Arrival: " + car.sendArrivalCnt.toCMVString());
            System.out.println("----------------------------- RECEIVE QUEUE Detail ----------------------------------------------");
            System.out.println("RecvQueue->SampleCnt: " + car.recvQueueSampleCnt.toCMVString());
            System.out.println("RecvQueue->QueueLen: " + car.recvQueueLen.toCMVString());
            System.out.println("RecvQueue->Arrival: " + car.recvArrivalCnt.toCMVString());
            System.out.println("----------------------------- Tuple Process Detail ----------------------------------------------");
            if (tupleProcessCnt > 0) {
                for (Map.Entry<String, CntMeanVar> innerE : car.tupleProcess.entrySet()) {
                    System.out.println(car.getProcessString() + "->" + innerE.getKey()+ ":" + innerE.getValue().toCMVString());
                }
            }
        }

        for (Map.Entry<String[], ComponentAggResult> e: boltResult.entrySet()){
            String[] cid = e.getKey();
            ComponentAggResult car = e.getValue();
            int tupleProcessCnt = car.tupleProcess.size();

            System.out.println("ComponentName: " + cid[0] + ", taskID: " + cid[1] + ", type: " + car.getComponentType() + ", tupleProcessCnt: " + tupleProcessCnt);
            System.out.println("----------------------------- SEND QUEUE Detail ----------------------------------------------");
            System.out.println("SendQueue->SampleCnt: " + car.sendQueueSampleCnt.toCMVString());
            System.out.println("SendQueue->QueueLen: " + car.sendQueueLen.toCMVString());
            System.out.println("SendQueue->Arrival: " + car.sendArrivalCnt.toCMVString());
            System.out.println("----------------------------- RECEIVE QUEUE Detail ----------------------------------------------");
            System.out.println("RecvQueue->SampleCnt: " + car.recvQueueSampleCnt.toCMVString());
            System.out.println("RecvQueue->QueueLen: " + car.recvQueueLen.toCMVString());
            System.out.println("RecvQueue->Arrival: " + car.recvArrivalCnt.toCMVString());
            System.out.println("----------------------------- Tuple Process Detail ----------------------------------------------");
            if (tupleProcessCnt > 0) {
                for (Map.Entry<String, CntMeanVar> innerE : car.tupleProcess.entrySet()) {
                    System.out.println(car.getProcessString() + "->" + innerE.getKey()+ ":" + innerE.getValue().toCMVString());
                }
            }
        }
    }
}
