package storm.resa.util;

import backtype.storm.Config;
import backtype.storm.serialization.DefaultKryoFactory;
import backtype.storm.serialization.SerializationFactory;
import backtype.storm.utils.Utils;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 14-6-7.
 */
public class TestSerialization {

    public static class KryoFactory extends DefaultKryoFactory {

        @Override
        public void postRegister(Kryo k, Map conf) {
        }
    }

    private Kryo kryo;
    private ByteArrayOutputStream output;

    @Before
    public void init() {
        Map conf = Utils.readStormConfig();
        conf.put(Config.TOPOLOGY_KRYO_FACTORY, KryoFactory.class.getName());
        kryo = SerializationFactory.getKryo(conf);
        kryo.setDefaultSerializer(FieldSerializer.class);
        output = new ByteArrayOutputStream();
    }

    @Test
    public void serializeMap() {
        Map<String, Integer> map = new HashMap<>(10);
        map.put("abc", 1);
        Output wrap = new Output(output);
        kryo.writeClassAndObject(wrap, map);
        wrap.close();
        System.out.println(output.size());
        map = (Map<String, Integer>) kryo.readClassAndObject(new Input(output.toByteArray()));
        Assert.assertEquals(1, (int) map.get("abc"));
    }


}
