package compress.util;

import compress.entities.base_char.MatchEntry;
import compress.entities.base_char.Bases_Seq;
import compress.entities.base_char.Ref_base;
import compress.entities.qualityS.QualityScores;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import org.apache.spark.serializer.KryoRegistrator;

public class mykryo implements KryoRegistrator {
    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(Ref_base.class, new FieldSerializer(kryo, Ref_base.class)); 
        kryo.register(Bases_Seq.class, new FieldSerializer(kryo, Bases_Seq.class));
        kryo.register(MatchEntry.class, new FieldSerializer(kryo, MatchEntry.class));
        kryo.register(QualityScores.class, new FieldSerializer(kryo, QualityScores.class));
    }
}
