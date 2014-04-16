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

    CntMeanVar totalProcessedTuple = null;

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

        totalProcessedTuple = new CntMeanVar();

        tupleProcess = new HashMap<>();
    }

    String getComponentType() {
        return type == ComponentType.bolt ? "bolt" : "spout";
    }

    String getProcessString() {
        return type == ComponentType.bolt ? "exec-delay" : "complete-latency";
    }

    CntMeanVar updateAndGetTotalProcessedTuple() {
        if (totalProcessedTuple == null) {
            totalProcessedTuple = new CntMeanVar();
        }
        totalProcessedTuple.clear();
        for (Map.Entry<String, CntMeanVar> e : tupleProcess.entrySet()) {
            if (e != null) {
                totalProcessedTuple.combine(e.getValue());
            }
        }
        return totalProcessedTuple;
    }

    CntMeanVar getTotalProcessedTuple() {
        return totalProcessedTuple;
    }

    static void combine(ComponentAggResult from, ComponentAggResult to) throws Exception {

        if (from != null && to != null) {
            if (from.type != to.type) {
                throw new Exception("Type is not the same");
            }

            to.recvArrivalCnt.combine(from.recvArrivalCnt);
            to.recvQueueLen.combine(from.recvQueueLen);
            to.recvQueueSampleCnt.combine(from.recvQueueSampleCnt);
            to.sendArrivalCnt.combine(from.sendArrivalCnt);
            to.sendQueueLen.combine(from.sendQueueLen);
            to.sendQueueSampleCnt.combine(from.sendQueueSampleCnt);

            ///Caution, this to.totalProcessedTuple is used for combined usage!
            to.totalProcessedTuple.combine(from.updateAndGetTotalProcessedTuple());
        }
    }
}
