package services.consumer;

import models.Operacao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class CapturaOperacao {

    public static void main(String[] args)
    {
        CapturaOperacao operService = new CapturaOperacao();
        try(KafkaService service = new KafkaService("teste", operService::parse, "integra_operacao", 1, "localhost:9092"))
        {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Operacao> record)
    {
        Headers headers = record.headers();

        for(Header header : headers)
        {
            System.out.println("Chave: " + header.key());
            System.out.println("Valor: " + header.value());
            header.value();
        }

        Operacao operacao = record.value();
        System.out.println("----------------------------");
        System.out.println("Operacao integrada no sistema. " +
                "Partition: "  + record.partition() + " - " +
                "OffSet: " + record.offset() + " - " +
                "Key: " + record.key() + " - " +
                "Value: " + record.value() + " - " +
                "TimeStamp: " + new Time((new Timestamp(record.timestamp()).getTime())) + " - " +
                "Operacao ID: " + operacao.getId() + " - " +
                "Operacao Referencia: " + operacao.getReferencia() + " - " +
                "Operacao Valor" + operacao.getValor());
    }
}
