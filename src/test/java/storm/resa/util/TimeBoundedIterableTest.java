package storm.resa.util;

import backtype.storm.utils.Utils;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by ding on 14-4-16.
 */
public class TimeBoundedIterableTest {

    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void testIterator() throws Exception {
        int duration = 1000;
        List<Integer> ints = IntStream.range(0, 10).boxed().collect(Collectors.toList());
        new TimeBoundedIterable<>(duration, ints).forEach(e -> {
            System.out.println(e);
            Utils.sleep(200);
        });
        System.out.println("--------");
        ints = IntStream.range(0, 3).boxed().collect(Collectors.toList());
        new TimeBoundedIterable<>(duration, ints).forEach(e -> {
            System.out.println(e);
            Utils.sleep(200);
        });
    }
}
