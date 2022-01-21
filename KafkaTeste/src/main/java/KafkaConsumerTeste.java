import java.sql.Timestamp;
import java.time.DateTimeException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerTeste {

    public static void main(String[] args) {


        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "grupo");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties))
        {
            consumer.subscribe(Arrays.asList("teste"));

            System.out.println("INICIO do consumo da fila KAFKA:");

            while(1==1)
            {
                ConsumerRecords<String, String> records =  consumer.poll(Duration.ofSeconds(2));

                for(ConsumerRecord<String, String> record : records)
                {
                    System.out.println("CHAVE: " + record.key() + " - " +
                            "VALOR: " + record.value() + " - " +
                            "TOPICO " + record.topic() + " - " +
                            "OFFSET " + record.offset() + " - " +
                            "PARTITION " + record.partition());
                }
            }

            //System.out.println("FIM do consumo da fila KAFKA");
        }
        catch(Exception ex)
        {
            System.out.println(ex.getMessage());
        }
    }
}