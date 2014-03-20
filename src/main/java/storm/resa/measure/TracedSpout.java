package storm.resa.measure;

import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import storm.resa.simulate.TupleCompletedMetric;

import java.util.List;
import java.util.Map;

/**
 * Created by ding on 14-3-19.
 */
public class TracedSpout implements IRichSpout {

    private static class MsgIdWrapper {
        final String traceId;
        final Object msgId;

        private MsgIdWrapper(String traceId, Object msgId) {
            this.traceId = traceId;
            this.msgId = msgId;
        }

    }

    private IRichSpout delegate;
    private TraceIdGenerator.OfSpout traceIdGenerator;
    private transient TupleCompletedMetric completedMetric;

    public TracedSpout(IRichSpout delegate, TraceIdGenerator.OfSpout traceIdGenerator) {
        this.delegate = delegate;
        this.traceIdGenerator = traceIdGenerator;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        completedMetric = context.registerMetric("tuple-completed", new TupleCompletedMetric(), 60);
        delegate.open(conf, context, new SpoutOutputCollector(new ISpoutOutputCollector() {
            @Override
            public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
                return collector.emit(streamId, tuple,
                        tupleStarted(traceIdGenerator.apply(streamId, tuple, messageId), messageId));
            }

            @Override
            public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
                collector.emitDirect(taskId, streamId, tuple,
                        tupleStarted(traceIdGenerator.apply(streamId, tuple, messageId), messageId));
            }

            @Override
            public void reportError(Throwable error) {
                collector.reportError(error);
            }

            private Object tupleStarted(String id, Object messageId) {
                if (id != null) {
                    completedMetric.tupleStarted(id);
                    return new MsgIdWrapper(id, messageId);
                }
                return messageId;
            }

        }));
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void activate() {
        delegate.activate();
    }

    @Override
    public void deactivate() {
        delegate.deactivate();
    }

    @Override
    public void nextTuple() {
        delegate.nextTuple();
    }

    @Override
    public void ack(Object msgId) {
        if (msgId instanceof MsgIdWrapper) {
            MsgIdWrapper wrapper = (MsgIdWrapper) msgId;
            completedMetric.tupleCompleted(wrapper.traceId);
            delegate.ack(wrapper.msgId);
        } else {
            delegate.ack(msgId);
        }
    }

    @Override
    public void fail(Object msgId) {
        if (msgId instanceof MsgIdWrapper) {
            MsgIdWrapper wrapper = (MsgIdWrapper) msgId;
            completedMetric.tupleFailed(wrapper.traceId);
            delegate.fail(wrapper.msgId);
        } else {
            delegate.fail(msgId);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        delegate.declareOutputFields(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return delegate.getComponentConfiguration();
    }
}

