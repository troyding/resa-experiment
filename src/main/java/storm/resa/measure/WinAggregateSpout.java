package storm.resa.measure;

import backtype.storm.Config;
import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.hooks.info.SpoutAckInfo;
import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * A measurable spout implementation based on system hook
 * <p>
 * Created by ding on 14-4-8.
 */
public class WinAggregateSpout implements IRichSpout {

    private class StreamMsgId {
        final String stream;
        final Object msgId;

        private StreamMsgId(String stream, Object msgId) {
            this.stream = stream;
            this.msgId = msgId;
        }
    }

    private class SpoutHook extends BaseTaskHook {

        @Override
        public void spoutAck(SpoutAckInfo info) {
            StreamMsgId streamMsgId = (StreamMsgId) info.messageId;
            if (streamMsgId != null && info.completeLatencyMs != null) {
                metric.addMetric(streamMsgId.stream, info.completeLatencyMs);
            }
        }
    }

    private IRichSpout delegate;
    private CMVMetric metric;

    public WinAggregateSpout(IRichSpout delegate) {
        this.delegate = delegate;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        metric = context.registerMetric("complete-latency", new CMVMetric(),
                Utils.getInt(conf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS)));
        context.addTaskHook(new SpoutHook());
        delegate.open(conf, context, new SpoutOutputCollector(new ISpoutOutputCollector() {

            @Override
            public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
                return collector.emit(streamId, tuple, newStreamMessageId(streamId, messageId));
            }

            @Override
            public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
                collector.emitDirect(taskId, streamId, tuple, newStreamMessageId(streamId, messageId));
            }

            @Override
            public void reportError(Throwable error) {
                collector.reportError(error);
            }

            private StreamMsgId newStreamMessageId(String stream, Object messageId) {
                return messageId == null ? null : new StreamMsgId(stream, messageId);
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

    private Object getUserMsgId(Object msgId) {
        return msgId != null ? ((StreamMsgId) msgId).msgId : msgId;
    }

    @Override
    public void ack(Object msgId) {
        delegate.ack(getUserMsgId(msgId));
    }

    @Override
    public void fail(Object msgId) {
        delegate.fail(getUserMsgId(msgId));
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
