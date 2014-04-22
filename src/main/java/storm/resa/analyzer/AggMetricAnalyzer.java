package storm.resa.analyzer;

import org.codehaus.jackson.map.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 14-3-4.
 * Note:
 * Recv-Queue arrival count includes ack for each message
 * When calculate sum and average, need to adjust (sum - #message, average - 1) for accurate value.
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

    private Map<String, ComponentAggResult> spoutResult = new HashMap<String, ComponentAggResult>();
    private Map<String, ComponentAggResult> boltResult = new HashMap<String, ComponentAggResult>();

    public void calCMVStat() {

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

                //String[] cid = new String[]{componentName, taskID};
                String cid = componentFullName;
                ComponentAggResult car = null;
                if (componentData.containsKey(TupleCompleteLatencyString)) {
                    car = spoutResult.get(cid);
                    if (car == null) {
                        car = new ComponentAggResult(ComponentAggResult.ComponentType.spout);
                        spoutResult.put(cid, car);
                    }
                } else if (componentData.containsKey(TupleExecString)) {
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

        ///printCMVStat(spoutResult);
        ///printCMVStat(boltResult);
    }

    public Map<String, ComponentAggResult> getSpoutResult () {
        return spoutResult;
    }

    public Map<String, ComponentAggResult> getBoltResult () {
        return boltResult;
    }

    public static void printCMVStat(Map<String, ComponentAggResult> result) {
        if (result == null) {
            System.out.println("input AggResult is null.");
            return;
        }

        for (Map.Entry<String, ComponentAggResult> e : result.entrySet()) {
            String cid = e.getKey();
            String componentName = cid.split(":")[0];
            String taskID = cid.split(":")[1];

            ComponentAggResult car = e.getValue();
            int tupleProcessCnt = car.tupleProcess.size();

            System.out.println("-------------------------------------------------------------------------------");
            System.out.println("ComponentName: " + componentName + ", taskID: " + taskID + ", type: " + car.getComponentType() + ", tupleProcessCnt: " + tupleProcessCnt);
            ///System.out.println("---------------------------------------------------------------------------");
            System.out.println("SendQueue->SampleCnt: " + car.sendQueueSampleCnt.toCMVString());
            System.out.println("SendQueue->QueueLen: " + car.sendQueueLen.toCMVString());
            System.out.println("SendQueue->Arrival: " + car.sendArrivalCnt.toCMVString());
            ///System.out.println("---------------------------------------------------------------------------");
            System.out.println("RecvQueue->SampleCnt: " + car.recvQueueSampleCnt.toCMVString());
            System.out.println("RecvQueue->QueueLen: " + car.recvQueueLen.toCMVString());
            System.out.println("RecvQueue->Arrival: " + car.recvArrivalCnt.toCMVString());
            ///System.out.println("---------------------------------------------------------------------------");
            if (tupleProcessCnt > 0) {
                for (Map.Entry<String, CntMeanVar> innerE : car.tupleProcess.entrySet()) {
                    System.out.println(car.getProcessString() + "->" + innerE.getKey() + ":" + innerE.getValue().toCMVString());
                }
            }
            System.out.println("-------------------------------------------------------------------------------");
        }

    }

    public static void printCMVStatShort(Map<String, ComponentAggResult> result) {
        if (result == null) {
            System.out.println("input AggResult is null.");
            return;
        }

        for (Map.Entry<String, ComponentAggResult> e : result.entrySet()) {
            String cid = e.getKey();
            String componentName = cid.split(":")[0];
            String taskID = cid.split(":")[1];

            ComponentAggResult car = e.getValue();
            int tupleProcessCnt = car.tupleProcess.size();

            System.out.print(componentName + ":" + taskID + ":" + car.getComponentType());
            System.out.print(",RQ:" + car.recvQueueLen.toCMVStringShort());
            System.out.print(",Arrl:" + car.recvArrivalCnt.toCMVStringShort());
            System.out.println(",SQ:" + car.sendQueueLen.toCMVStringShort());

            if (tupleProcessCnt > 0) {
                for (Map.Entry<String, CntMeanVar> innerE : car.tupleProcess.entrySet()) {
                    System.out.println(car.getProcessString() + "->" + innerE.getKey() + ":" + innerE.getValue().toCMVString());
                }
            }
            System.out.println("-------------------------------------------------------------------------------");
        }
    }
}
