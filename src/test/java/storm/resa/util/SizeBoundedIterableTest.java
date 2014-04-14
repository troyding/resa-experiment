package storm.resa.util;

import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by ding on 14-4-14.
 */
public class SizeBoundedIterableTest {

    @Test
    public void testIterator() throws Exception {
        List<Integer> ints = IntStream.range(0, 10).boxed().collect(Collectors.toList());
        new SizeBoundedIterable<>(5, ints).forEach(System.out::println);

        ints = IntStream.range(0, 3).boxed().collect(Collectors.toList());
        new SizeBoundedIterable<>(5, ints).forEach(System.out::println);
    }
}
