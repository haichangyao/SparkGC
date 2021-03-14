package pojo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import org.apache.spark.serializer.KryoRegistrator;
import pojo.compression_type;
import pojo.reference_type;

public class mykryo implements KryoRegistrator {
    @Override
    public void registerClasses(Kryo kryo) {
        //register reference class in Kryo
        kryo.register(reference_type.class, new FieldSerializer(kryo, reference_type.class));

        //register target class in Kryo
        kryo.register(compression_type.class, new FieldSerializer(kryo,compression_type.class));

        //register matchEntry in Kryo
        kryo.register(MatchEntry.class, new FieldSerializer(kryo,MatchEntry.class));
    }
}
