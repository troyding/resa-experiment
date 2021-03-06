package storm.resa.util;

import backtype.storm.Config;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 14-1-16.
 */
public class ConfigUtil {

    public static Config readConfig(InputStream in) {
        Yaml yaml = new Yaml();
        Map ret = (Map) yaml.load(new InputStreamReader(in));
        if (ret == null) {
            return null;
        }
        Config conf = new Config();
        conf.putAll(ret);
        return conf;
    }


    public static Config readConfig(File f) {
        try {
            return readConfig(new FileInputStream(f));
        } catch (FileNotFoundException e) {
            return null;
        }
    }

    public static int getInt(Map<String, Object> conf, String key, int defaultValue) {
        Object value = conf.get(key);
        if (value != null && value instanceof Number) {
            return ((Number) value).intValue();
        }
        return defaultValue;
    }

    public static int getIntThrow(Map<String, Object> conf, String key) {
        return getNumberThrow(conf, key).intValue();
    }

    public static double getDoubleThrow(Map<String, Object> conf, String key) {
        return getNumberThrow(conf, key).doubleValue();
    }

    private static Number getNumberThrow(Map<String, Object> conf, String key) {
        Object value = conf.get(key);
        if (value != null && value instanceof Number) {
            return (Number) value;
        }
        throw new IllegalStateException("no " + key + " found in this conf");
    }

    public static double getDouble(Map<String, Object> conf, String key, double defaultValue) {
        Object value = conf.get(key);
        if (value != null && value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return defaultValue;
    }

    public static boolean getBoolean(Map<String, Object> conf, String key, boolean defaultValue) {
        Object value = conf.get(key);
        if (value != null && value instanceof Boolean) {
            return ((Boolean) value).booleanValue();
        }
        return defaultValue;
    }

    public static void printConfig(Map<String, Object> conf) {
        if (conf == null) {
            System.out.println("input configure is null");
        } else {
            for (Map.Entry<String, Object> e : conf.entrySet()) {
                System.out.println(e.getKey() + " : " + e.getValue());
            }
        }
    }
}
