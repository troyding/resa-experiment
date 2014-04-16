package storm.resa.util;

import java.util.Iterator;
import java.util.Objects;

/**
 * Created by ding on 14-4-16.
 */
public class TimeBoundedIterable<T> implements Iterable<T> {

    private Iterator<T> input;
    private long stopTimeStamp;

    public TimeBoundedIterable(long duration, Iterable<T> input) {
        this.input = Objects.requireNonNull(input).iterator();
        this.stopTimeStamp = System.currentTimeMillis() + duration;
    }

    @Override
    public Iterator<T> iterator() {
        return new Iter();
    }

    private class Iter implements Iterator<T> {

        @Override
        public boolean hasNext() {
            return System.currentTimeMillis() < stopTimeStamp && input.hasNext();
        }

        @Override
        public T next() {
            return input.next();
        }
    }

}