package storm.resa.metric;

public class MetricName {

    // strom build-in
    private static final String BUILDIN_METRIC_PREFIX = "__";

    public static final String EXECUTE_LATENCY = BUILDIN_METRIC_PREFIX + "execute-latency";

    public static final String PROCESS_LATENCY = BUILDIN_METRIC_PREFIX + "process-latency";

    public static final String ACK_COUNT = BUILDIN_METRIC_PREFIX + "ack-count";

    public static final String FAIL_COUNT = BUILDIN_METRIC_PREFIX + "fail-count";

    public static final String EMIT_COUNT = BUILDIN_METRIC_PREFIX + "emit-count";

    public static final String EXECUTE_COUNT = BUILDIN_METRIC_PREFIX + "execute-count";

    public static final String COMPLETE_LATENCY = BUILDIN_METRIC_PREFIX + "complete-latency";

    // add by resa
    public static final String TRANSFER_LATENCY = BUILDIN_METRIC_PREFIX + "transfer-latency";

}
