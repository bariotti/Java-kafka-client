package services.consumer;

import Util.JsonDeserializer;
import models.Operacao;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

public class KafkaService<T> implements Closeable {

    private final KafkaConsumer<String, T> consumer;
    private final String topico;
    private final ConsumerFunction parse;
    private final String grupo;
    private final int maxPollSize;
    private final String server;

    public KafkaService(String topico, ConsumerFunction parse, String grupo, int maxPollSize, String server) {
        this.topico = topico;
        this.parse = parse;
        this.grupo = grupo;
        this.maxPollSize = maxPollSize;
        this.server = server;
        this.consumer = new KafkaConsumer<String, T>(properties());
        consumer.subscribe(Collections.singleton(this.topico));
    }

    public void run()
    {
        System.out.println("Inicio do consumo do topico Kafka -->" +
                " Servidor: " + server +
                " Topico: " + topico +
                " Max Poll Size: " + maxPollSize +
                " Grupo: " + grupo);

        while(true)
        {
            ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(100));
            if(!records.isEmpty())
            {
                System.out.println("Registros encontrados: " +records.count());

                for(ConsumerRecord<String, T> record : records)
                {
                    parse.consume(record);
                    consumer.commitSync();
                }
            }
        }
    }

    private Properties properties()
    {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, grupo);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollSize));
        properties.put(JsonDeserializer.TYPE_CONFIG, Operacao.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return properties;
    }

    @Override
    public void close()  {
        consumer.close();
    }
}
