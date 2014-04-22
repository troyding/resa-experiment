package storm.resa.analyzer;

import java.util.HashMap;
import java.util.Map;

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

    static void combine(ComponentAggResult from, ComponentAggResult to) throws Exception {

        if (from != null && to != null) {
            if (from.type != to.type) {
                throw new Exception("Type is not the same");
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
                        to.tupleProcess.put(e.getKey(), e.getValue());
                    }
                }
            }
        }
    }

    void addCAR(ComponentAggResult car) throws Exception {
        if (car != null) {
            if (this.type != car.type) {
                throw new Exception("Type is not the same");
            }

            this.recvArrivalCnt.addCMV(car.recvArrivalCnt);
            this.recvQueueLen.addCMV(car.recvQueueLen);
            this.recvQueueSampleCnt.addCMV(car.recvQueueSampleCnt);
            this.sendArrivalCnt.addCMV(car.sendArrivalCnt);
            this.sendQueueLen.addCMV(car.sendQueueLen);
            this.sendQueueSampleCnt.addCMV(car.sendQueueSampleCnt);

            if (car.tupleProcess != null) {
                for (Map.Entry<String, CntMeanVar> e : car.tupleProcess.entrySet()) {
                    if (this.tupleProcess.containsKey(e.getKey())) {
                        this.tupleProcess.get(e.getKey()).addCMV(e.getValue());
                    } else {
                        this.tupleProcess.put(e.getKey(), e.getValue());
                    }
                }
            }
        }
    }
}
