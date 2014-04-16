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
}
