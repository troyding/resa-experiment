package storm.resa.analyzer;

import redis.clients.jedis.Jedis;

import java.util.Iterator;

/**
 * Created by ding on 14-3-4.
 */
public class JedisResource implements Iterable<Object> {

    private String host;
    private int port;
    private String queue;
    private Jedis jedis;

    public JedisResource(String host, int port, String queue) {
        this.host = host;
        this.port = port;
        this.queue = queue;
        jedis = new Jedis(host, port);
    }

    private class DataIter implements Iterator<Object> {

        private Object curr = null;

        @Override
        public boolean hasNext() {
            curr = getJedis().lpop(queue);
            if (curr != null) {
                return true;
            } else {
                jedis.disconnect();
                curr = null;
                return false;
            }
        }

        @Override
        public Object next() {
            return curr;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private Jedis getJedis() {
        return jedis;
    }

    @Override
    public Iterator<Object> iterator() {
        return new DataIter();
    }

}
