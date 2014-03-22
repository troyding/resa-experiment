package storm.resa.analyzer;

import clojure.main;
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
            return curr != null;
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

    public static void main(String[] args) {
        ///MetricAnalyzer analyzer = new MetricAnalyzer(new JedisResource(args[0], Integer.valueOf(args[1]), args[2]));
        ///analyzer.calcAvg();
        AggMetricAnalyzer aggAnalyzer = new AggMetricAnalyzer(new JedisResource(args[0], Integer.valueOf(args[1]), args[2]));
        aggAnalyzer.calcAvg();
    }
}
