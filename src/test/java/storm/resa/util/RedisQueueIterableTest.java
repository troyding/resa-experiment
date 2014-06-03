package storm.resa.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class RedisQueueIterableTest {

    @Test
    public void testIterator() throws Exception {
        int maxCount = 10;
        Iterable<String> source = new RedisQueueIterable("192.168.0.30", 6379, "wc-counter-4", maxCount);
        List<String> ret = StreamSupport.stream(source.spliterator(), false).collect(Collectors.toList());
        Assert.assertEquals(ret.size(), maxCount);
    }
}