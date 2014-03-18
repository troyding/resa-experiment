package storm.resa.util;

/**
 * Created by ding on 14-3-14.
 */
public class Counter {

    private long count;

    public Counter() {
        this(0);
    }

    public Counter(long count) {
        this.count = count;
    }

    public long incAndGet() {
        return ++count;
    }

    public long getAndInc() {
        return count++;
    }

    public long get() {
        return count;
    }
}


