package storm.resa.simulate;

import backtype.storm.metric.api.IMetric;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ding on 14-1-28.
 */
public class ExecuteMetric implements IMetric {

    private Map<String, List<String>> data = new HashMap<String, List<String>>();

    public void addMetric(String key, String value) {
        List<String> l = data.get(key);
        if (l == null) {
            l = new ArrayList<String>(3);
            data.put(key, l);
        }
        l.add(value);
    }

    @Override
    public Object getValueAndReset() {
        Map<String, List<String>> ret = data;
        data = new HashMap<String, List<String>>();
        return ret;
    }
}
