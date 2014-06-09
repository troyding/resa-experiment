package storm.resa.app.fp;

import org.junit.Test;
import storm.resa.app.cod.DataSender;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class DataSenderAddTest {

    @Test
    public void testSend2Queue() throws Exception {
        String inputFile = this.getClass().getClassLoader().getResource("fpInput.txt").getPath();
        Map<String, Object> conf = new HashMap<>();
        conf.put("redis.host", "192.168.0.30");
        conf.put("redis.port", 6379);
        conf.put("redis.queue", "sentences");

        DataSender sender = new DataSender(conf);



    }
}