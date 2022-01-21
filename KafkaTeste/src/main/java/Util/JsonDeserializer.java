package Util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import java.util.Map;

public class JsonDeserializer<T> implements Deserializer {

    public static final String TYPE_CONFIG = "type_config";

    private final Gson gson = new GsonBuilder().create();
    private Class<T> type;

    @Override
    public void configure(Map configs, boolean isKey) {

        String typeName = String.valueOf(configs.get(TYPE_CONFIG));

        try
        {
            type = (Class<T>) Class.forName(typeName);
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException("Erro ao instanciar a classe " + typeName);
        }
    }

    @Override
    public Object deserialize(String topic, byte[] data)
    {
        return gson.fromJson(new String(data), type);
    }
}
