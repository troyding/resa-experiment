package storm.resa.app.fp;

import org.junit.Test;
import storm.resa.app.cod.DataSender;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class DataSenderAddTest {

    @Test
    public void testSend2Queue() throws Exception {
        URI inputFile = this.getClass().getResource("/fpin.txt").toURI();
        Map<String, Object> conf = new HashMap<>();
        conf.put("redis.host", "192.168.0.30");
        conf.put("redis.port", 6379);
        conf.put("redis.queue", "sentences");

        DataSenderAdd sender = new DataSenderAdd(conf);

        long sleep =1000;
        sender.send2Queue(Paths.get(inputFile), () -> sleep);

    }
}