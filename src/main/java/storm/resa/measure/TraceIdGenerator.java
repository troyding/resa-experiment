package storm.resa.measure;

import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.List;
import java.util.function.Function;

/**
 * Created by ding on 14-3-19.
 */
public interface TraceIdGenerator extends Serializable {

    @FunctionalInterface
    public static interface OfSpout extends TraceIdGenerator {

        String apply(String streamId, List<Object> tuple, Object messageId);

    }

    @FunctionalInterface
    public static interface OfBolt extends TraceIdGenerator, Function<Tuple, String> {

    }

}
