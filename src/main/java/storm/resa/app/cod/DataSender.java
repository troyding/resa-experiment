package storm.resa.app.cod;

import backtype.storm.utils.Utils;
import redis.clients.jedis.Jedis;
import storm.resa.util.ConfigUtil;
import storm.resa.util.Counter;

import java.io.*;
import java.nio.file.*;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ding on 14-3-18.
 */
public class DataSender {

    private String host;
    private int port;
    private String queueName;

    public DataSender(Map<String, Object> conf) {
        this.host = (String) conf.get("redis.host");
        this.port = ((Number) conf.get("redis.port")).intValue();
        this.queueName = (String) conf.get("redis.queue");
    }

    public void send2Queue(Path inputFile, float rate) throws IOException {
        Jedis jedis = new Jedis(host, port);
        AtomicLong counter = new AtomicLong(0);
        long sleep = (long) (1000 / rate);
        try (BufferedReader reader = Files.newBufferedReader(inputFile)) {
            reader.lines().forEach((line) -> {
                Utils.sleep(sleep);
                String data = counter.getAndIncrement() + "|" + System.currentTimeMillis() + "|" + line;
                jedis.rpush(queueName, data);
            });
        } finally {
            jedis.quit();
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.println("usage: DataSender <confFile> <inputFile> <rate>");
            return;
        }
        DataSender sender = new DataSender(ConfigUtil.readConfig(new File(args[0])));
        System.out.println("start sender");
        sender.send2Queue(Paths.get(args[1]), Float.parseFloat(args[2]));
        System.out.println("end sender");
    }

}
