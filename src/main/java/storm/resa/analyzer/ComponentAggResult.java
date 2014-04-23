package storm.resa.analyzer;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

/**
 * Created by Tom.fu on 16/4/2014.
 */
public class ComponentAggResult {
    CntMeanVar recvArrivalCnt = null;
    CntMeanVar recvQueueLen = null;
    CntMeanVar recvQueueSampleCnt = null;

    CntMeanVar sendArrivalCnt = null;
    CntMeanVar sendQueueLen = null;
    CntMeanVar sendQueueSampleCnt = null;

    Map<String, CntMeanVar> tupleProcess = null;
    enum ComponentType {bolt, spout};

    ComponentType type = null;

    ComponentAggResult(ComponentType t) {
        this.type = t;
        recvArrivalCnt = new CntMeanVar();
        recvQueueLen = new CntMeanVar();
        recvQueueSampleCnt = new CntMeanVar();

        sendArrivalCnt = new CntMeanVar();
        sendQueueLen = new CntMeanVar();
        sendQueueSampleCnt = new CntMeanVar();

        tupleProcess = new HashMap<>();
    }

    String getComponentType() {
        return type == ComponentType.bolt ? "bolt" : "spout";
    }
    String getProcessString() {
        return type == ComponentType.bolt ? "exec-delay" : "complete-latency";
    }

    CntMeanVar getSimpleCombinedProcessedTuple() {

        CntMeanVar retVal = new CntMeanVar();

        for (Map.Entry<String, CntMeanVar> e : this.tupleProcess.entrySet()) {
            if (e != null) {
                retVal.addCMV(e.getValue());
            }
        }
        return retVal;
    }

    static void addCARto(ComponentAggResult from, ComponentAggResult to) {

        if (from != null && to != null) {
            if (from.type != to.type) {
                System.out.println("Type is not the same");
                return;
            }

            to.recvArrivalCnt.addCMV(from.recvArrivalCnt);
            to.recvQueueLen.addCMV(from.recvQueueLen);
            to.recvQueueSampleCnt.addCMV(from.recvQueueSampleCnt);
            to.sendArrivalCnt.addCMV(from.sendArrivalCnt);
            to.sendQueueLen.addCMV(from.sendQueueLen);
            to.sendQueueSampleCnt.addCMV(from.sendQueueSampleCnt);

            if (from.tupleProcess != null) {
                for (Map.Entry<String, CntMeanVar> e : from.tupleProcess.entrySet()) {
                    if (to.tupleProcess.containsKey(e.getKey())) {
                        to.tupleProcess.get(e.getKey()).addCMV(e.getValue());
                    } else {
                        CntMeanVar cmv = new CntMeanVar();
                        cmv.addCMV(e.getValue());
                        to.tupleProcess.put(e.getKey(), cmv);
                    }
                }
            }
        }
    }

    static ComponentAggResult getSimpleCombinedHistory(Queue<ComponentAggResult> his, ComponentType t) {
        ComponentAggResult ret = new ComponentAggResult(t);
        for (ComponentAggResult hisCar : his) {
            ret.addCAR(hisCar);
        }
        return ret;
    }

    void addCAR(ComponentAggResult car) {
        if (car != null) {
            if (this.type != car.type) {
                System.out.println("Type is not the same");
                return;
            }

            this.recvArrivalCnt.addCMV(car.recvArrivalCnt);
            this.recvQueueLen.addCMV(car.recvQueueLen);
            this.recvQueueSampleCnt.addCMV(car.recvQueueSampleCnt);
            this.sendArrivalCnt.addCMV(car.sendArrivalCnt);
            this.sendQueueLen.addCMV(car.sendQueueLen);
            this.sendQueueSampleCnt.addCMV(car.sendQueueSampleCnt);

            if (car.tupleProcess != null) {
                for (Map.Entry<String, CntMeanVar> e : car.tupleProcess.entrySet()) {
                    ///for test
                    ///System.out.println("fortest: " + e.getKey() + "," + e.getValue().toCMVString());

                    if (this.tupleProcess.containsKey(e.getKey())) {
                        this.tupleProcess.get(e.getKey()).addCMV(e.getValue());
                    } else {
                        CntMeanVar cmv = new CntMeanVar();
                        cmv.addCMV(e.getValue());
                        this.tupleProcess.put(e.getKey(), cmv);
                    }
                }
            }
        }
    }
}
