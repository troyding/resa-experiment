package storm.resa.tools;

import org.apache.commons.io.IOUtils;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * Created by ding on 14-1-15.
 */
public class QueueSimulator {

    private String host;
    private int port;

    public QueueSimulator(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void simulate(File inputFile, String queueName, float rate, int repeatTimes) throws IOException {
        Jedis jedis = new Jedis(host, port);
        List<String> lines = (List<String>) IOUtils.readLines(new FileInputStream(inputFile));
        long sleep = (long) (1000 / rate);
        Random rand = new Random();
        try {
            for (int i = 0; i < repeatTimes; i++) {
                jedis.rpush(queueName, lines.get(rand.nextInt(lines.size())));
                Thread.sleep(sleep);
            }
        } catch (InterruptedException e) {
        } finally {
            jedis.quit();
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 6) {
            System.out.println("usage: QueueSimulator <host> <ip> <inputFile> <queue> <rate> <repeatTimes>");
        }
        QueueSimulator simulator = new QueueSimulator(args[0], Integer.parseInt(args[1]));
        System.out.println("start simulate");
        simulator.simulate(new File(args[2]), args[3], Float.parseFloat(args[4]), Integer.parseInt(args[5]));
        System.out.println("end simulate");
    }


}
