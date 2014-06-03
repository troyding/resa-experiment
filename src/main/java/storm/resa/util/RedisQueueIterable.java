package storm.resa.util;

import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.List;

/**
 * This class is not thread-safe
 * Created by ding on 14-5-29.
 */
public class RedisQueueIterable implements Iterable<String> {

    private String host;
    private int port;
    private String queue;
    private Jedis jedis;
    private long maxCount;
    private long count = 0;
    private final int bufferSize = 50;

    public RedisQueueIterable(String host, int port, String queue) {
        this(host, port, queue, Long.MAX_VALUE);
    }

    public RedisQueueIterable(String host, int port, String queue, long maxCount) {
        this.host = host;
        this.port = port;
        this.queue = queue;
        this.maxCount = maxCount;
    }

    private String[] fetchNextRange() {
        if (count < maxCount) {
            long bound = Math.min(maxCount, count + bufferSize) - 1;
            try {
                List<String> nextRange = getJedis().lrange(queue, count, bound);
                if (!nextRange.isEmpty()) {
                    return nextRange.toArray(new String[nextRange.size()]);
                }
            } catch (Exception e) {
            }
        }
        disconnect();
        return null;
    }

    private class DataIter implements Iterator<String> {

        private String[] cache = null;
        private int pos = 0;

        @Override
        public boolean hasNext() {
            if (cache == null || pos == cache.length) {
                cache = fetchNextRange();
                if (cache == null) {
                    return false;
                }
                pos = 0;
            }
            return true;
        }

        @Override
        public String next() {
            count++;
            return cache[pos++];
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private Jedis getJedis() {
        if (jedis != null) {
            return jedis;
        }
        //try connect to redis server
        try {
            jedis = new Jedis(host, port);
        } catch (Exception e) {
        }
        return jedis;
    }

    private void disconnect() {
        if (jedis != null) {
            try {
                jedis.disconnect();
            } catch (Exception e) {
            }
            jedis = null;
        }
    }

    @Override
    public Iterator<String> iterator() {
        return new DataIter();
    }

}
