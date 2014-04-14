package storm.resa.util;

import java.util.Iterator;
import java.util.Objects;
import java.util.stream.StreamSupport;

/**
 * Iterate over a given iterator to get a fixed number of element
 * <p>
 * Created by ding on 14-4-14.
 */
public class SizeBoundedIterable<T> implements Iterable<T> {

    private Iterable<T> input;
    private int maxSize;

    public SizeBoundedIterable(int maxSize, Iterable<T> input) {
        this.input = Objects.requireNonNull(input);
        this.maxSize = maxSize;
    }

    @Override
    public Iterator<T> iterator() {
        return StreamSupport.stream(input.spliterator(), false).limit(maxSize).iterator();
    }

}
