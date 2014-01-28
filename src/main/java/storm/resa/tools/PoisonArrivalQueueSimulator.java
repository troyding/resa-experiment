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
public class PoisonArrivalQueueSimulator {

    private String host;
    private int port;

    public PoisonArrivalQueueSimulator(String host, int port) {
        this.host = host;
        this.port = port;
    }

    ///lambda is the parameter controlling the arrival rate,
    ///and 1/lambda is the expected interarrival time    
    public void simulate(File inputFile, String queueName, float lambda, int repeatTimes, String measureQueueName) throws IOException {
        Jedis jedis = new Jedis(host, port);
        List<String> lines = (List<String>) IOUtils.readLines(new FileInputStream(inputFile));

        Random rand = new Random();
        try {
            if (repeatTimes < 0) {
                System.out.println("Negative repeatTimes, infinit mode, Lambda: " + lambda);
                while (true) {
                    jedis.rpush(queueName, lines.get(rand.nextInt(lines.size())));
                    //Thread.sleep(sleep);
                    double inter = -Math.log(rand.nextDouble()) * 1000.0 / (double) lambda;
                    long interarrival = (long) inter;
                    Thread.sleep(interarrival);
                }
            } else {
                System.out.print("RepeatTimes: " + repeatTimes + ", Lambda: " + lambda);
                if (measureQueueName != null) {
                    System.out.println(", Measured into queue: " + measureQueueName);
                } else {
                    System.out.println(", no measurement queue specified.");
                }
                for (int i = 0; i < repeatTimes; i++) {
                    jedis.rpush(queueName, lines.get(rand.nextInt(lines.size())));
                    ///Thread.sleep(sleep);
                    double inter = -Math.log(rand.nextDouble()) * 1000.0 / (double) lambda;
                    long interarrival = (long) inter;
                    if (measureQueueName != null) {
                        jedis.rpush(measureQueueName, String.valueOf(inter));
                    }
                    Thread.sleep(interarrival);
                }
            }
        } catch (InterruptedException e) {
        } finally {
            jedis.quit();
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 6) {
            System.out.println("usage: QueueSimulator <host> <ip> <inputFile> <queue> <rate> <repeatTimes> [<measureQueue>]");
            return;
        }
        PoisonArrivalQueueSimulator simulator = new PoisonArrivalQueueSimulator(args[0], Integer.parseInt(args[1]));
        System.out.println("start simulate");
        String measureQueue = null;
        if (args.length == 7) {
            measureQueue = args[6];
        }
        simulator.simulate(new File(args[2]), args[3], Float.parseFloat(args[4]), Integer.parseInt(args[5]), measureQueue);
        System.out.println("end simulate");
    }


}
