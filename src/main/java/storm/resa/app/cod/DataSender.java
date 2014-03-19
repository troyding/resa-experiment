package storm.resa.app.cod;

import backtype.storm.utils.Utils;
import redis.clients.jedis.Jedis;
import storm.resa.util.ConfigUtil;
import storm.resa.util.Counter;

import java.io.*;
import java.nio.file.*;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

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

    public void send2Queue(Path inputFile, LongSupplier sleep) throws IOException {
        Jedis jedis = new Jedis(host, port);
        AtomicLong counter = new AtomicLong(0);
        try (BufferedReader reader = Files.newBufferedReader(inputFile)) {
            reader.lines().forEach((line) -> {
                long ms = sleep.getAsLong();
                if (ms > 0) {
                    Utils.sleep(ms);
                }
                String data = counter.getAndIncrement() + "|" + System.currentTimeMillis() + "|" + line;
                jedis.rpush(queueName, data);
            });
        } finally {
            jedis.quit();
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.println("usage: DataSender <confFile> <inputFile> [-avg <rate>] [-poison <lambda>]");
            return;
        }
        DataSender sender = new DataSender(ConfigUtil.readConfig(new File(args[0])));
        System.out.println("start sender");
        Path dataFile = Paths.get(args[1]);
        switch (args[2].substring(1)) {
            case "avg":
                long sleep = (long) (1000 / Float.parseFloat(args[3]));
                sender.send2Queue(dataFile, () -> sleep);
                break;
            case "poison":
                double lambda = Float.parseFloat(args[3]);
                sender.send2Queue(dataFile, () -> (long) (-Math.log(Math.random()) * 1000 / lambda));
                break;
            default:
                sender.send2Queue(dataFile, () -> 0);
                break;
        }
        System.out.println("end sender");
    }

}
