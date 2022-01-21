package services.producer;

import Util.JsonSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaDispatcher<T>  implements Closeable {

    private final String server;
    private final String topico;
    private final KafkaProducer<String, T> producer;

    public KafkaDispatcher(String server, String topico)
    {
        this.server = server;
        this.topico = topico;
        this.producer = new KafkaProducer<>(getProperties());
    }

    public void send(String key, T value, Map<String, String> headers) throws ExecutionException, InterruptedException {

        ProducerRecord<String, T> record = new ProducerRecord<>(this.topico, key, value);

        if(headers != null && headers.size() > 0)
        {
            for(Map.Entry<String, String> header : headers.entrySet())
            {
                record.headers().add(new RecordHeader(header.getKey(), header.getValue().getBytes()));
            }
        }

        Callback callback = (data, ex) -> {
            if(ex != null)
            {
                System.out.println("Erro ao enviar mensagem para o " +
                        " topico: " + topico +
                        " servidor: " + server +
                        " chave:  " + key +
                        " valor: " + value +
                        ". ERRO: " + ex.getMessage());

                return;
            }

            System.out.println("Mensagem enviada com sucesso!" +
                    " valor: " + value +
                    " topico: " + data.topic() +
                    " offset: " + data.offset() +
                    " partition: " + data.partition() +
                    " timestamp" + data.timestamp());
        };

        producer.send(record, callback);
    }

    private Properties getProperties()
    {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.server);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        //properties.put(ProducerConfig.LINGER_MS_CONFIG, 1000);
        //properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 100);

        return properties;
    }


    @Override
    public void close()  {
        producer.close();
    }
}
