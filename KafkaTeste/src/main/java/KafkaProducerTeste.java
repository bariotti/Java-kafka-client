
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerTeste {

    public static void main(String[] args) throws InterruptedException, ExecutionException
    {
        producer1("teste", "1", "Mensagem 1");
        producer2("teste", "2", "Mensagem 2");
        producer3("teste", "3", "Mensagem 3");
        producer4("teste", "4", "Mensagem 4");
        producer5("teste_2", "5", "Mensagem 5");



    }

    public static Properties getProperties()
    {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }


    /**
     * Metodo para produzir mensagem ao um topico do Kafka no qual implementa o TRY
     * De maneira implicita, o metodo CLOSE é chamada e a mensagem é enviada ao topico
     * @param topic
     * @param key
     * @param message
     */
    public static void producer1(String topic, String key, String message)
    {
        try(KafkaProducer<String, String> producer = new KafkaProducer<String, String>(getProperties()))
        {
            ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, key, message);

            producer.send(msg);

        }
    }

    /**
     * Metodo para produzir mensagem a um topcio do kafka
     * A mensagem é enviada após passar pelo metodo CLOSE.
     * Caso o mesmo não seja chamado, a mensagem não é enviada
     * @param topic
     * @param key
     * @param message
     */
    public static void producer2(String topic, String key, String message)
    {
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(getProperties());

        ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, key, message);

        producer.send(msg);

        producer.close();
    }

    /**
     * Metodo para produzir mensagem a um topico do Kafka
     * utilizando Callback
     * @param topic
     * @param key
     * @param message
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static void producer3(String topic, String key, String message) throws InterruptedException, ExecutionException
    {
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(getProperties());

        ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, key, message);

        producer.send(msg, new Callback()
        {
            public void onCompletion(RecordMetadata r, Exception ex)
            {
                if(ex != null)
                {
                    System.out.println("Erro ao enviar mensagem. " + ex.getMessage());
                    return;
                }

                System.out.println("Mensagem enviada com sucesso. TOPIC: " + r.topic() + " - OFFSET: " + r.offset() +  " - PARTITION: " + r.partition());
            }
        }).get();
    }

    /**
     * Metodo para produzir mensagem a um topico do Kafka
     * utilizando Callback simplificado (arrow function)
     * @param topic
     * @param key
     * @param message
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static void producer4(String topic, String key, String message) throws InterruptedException, ExecutionException
    {
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(getProperties());

        ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, key, message);

        producer.send(msg, (r, ex) -> {

            if(ex != null)
            {
                System.out.println("Erro ao enviar mensagem. " + ex.getMessage());
                return;
            }

            System.out.println("Mensagem enviada com sucesso. TOPIC: " + r.topic() + " - OFFSET: " + r.offset() +  " - PARTITION: " + r.partition());
        }).get();

    }

    public static void producer5(String topic, String key, String message) throws InterruptedException, ExecutionException
    {
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(getProperties());

        ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, key, message);

        Callback callback = (r, ex) -> {
            if(ex != null)
            {
                System.out.println("Erro ao enviar mensagem. " + ex.getMessage());
                return;
            }

            System.out.println("Mensagem enviada com sucesso. TOPIC: " + r.topic() + " - OFFSET: " + r.offset() +  " - PARTITION: " + r.partition());
        };

        producer.send(msg, callback).get();

    }
}
