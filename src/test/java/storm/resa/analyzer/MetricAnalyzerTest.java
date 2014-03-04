package storm.resa.analyzer;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.FileReader;
import java.io.IOException;

/**
 * Created by ding on 14-3-4.
 */
public class MetricAnalyzerTest {

    private MetricAnalyzer analyzer;

    @Before
    public void init() throws IOException {
        analyzer = new MetricAnalyzer(IOUtils.readLines(new FileReader("/Volumes/Data/tmp/resa/metrics.txt")));
    }

    @Test
    public void calcAvg() throws Exception {
        analyzer.calcAvg();
    }

}
