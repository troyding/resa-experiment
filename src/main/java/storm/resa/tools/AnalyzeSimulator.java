package storm.resa.tools;

import org.apache.commons.io.IOUtils;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by ding on 14-1-15.
 */
public class AnalyzeSimulator {

    private String host;
    private int port;
    private double sum;
    private int count;

    public AnalyzeSimulator(String host, int port) {
        this.host = host;
        this.port = port;
        sum = 0;
        count = 0;
    }

    public void simulate(File outputFile, String queueName) throws IOException {
        Jedis jedis = new Jedis(host, port);

        try {
            Object text = null;
            while ((text = jedis.lpop(queueName)) != null) {
                String t = String.valueOf(text);
                IOUtils.write(t + "\n", new FileOutputStream(outputFile, true));
                double val = Double.valueOf(t);
                sum += val;
                count++;
            }
        } catch (Exception e) {
        } finally {
            jedis.quit();
        }
    }

    public int getCount() {
        return this.count;
    }

    public double getSum() {
        return this.sum;
    }

    public double getAvg() {
        return (count > 0) ? (sum / (double) count) : 0.0;
    }


    public static void main(String[] args) throws IOException {
        if (args.length < 4) {
            System.out.println("usage: AnalyzeSimulator <host> <ip> <outputFile> <queueName>");
            return;
        }
        AnalyzeSimulator simulator = new AnalyzeSimulator(args[0], Integer.parseInt(args[1]));
        System.out.println("start analyze");
        simulator.simulate(new File(args[2]), args[3]);

        System.out.println("end analyze, count: " + String.valueOf(simulator.getCount()) + ", sum: " +
                String.valueOf(simulator.getSum()) + ", avg: " + simulator.getAvg());
    }
}
