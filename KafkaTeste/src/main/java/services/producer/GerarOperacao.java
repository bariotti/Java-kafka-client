package services.producer;

import com.oracle.jrockit.jfr.DurationEvent;
import models.Operacao;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class GerarOperacao {

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {

        LocalDateTime inicio = LocalDateTime.now();

        try(KafkaDispatcher dispatcher = new KafkaDispatcher("localhost:9092", "teste")) {
            for (int i = 1; i <= 3; i++) {

                 String key = UUID.randomUUID().toString();

                Random random = new Random();
                int id = random.nextInt();
                double valor = Math.random() * 5000 + 1;
                String referencia = UUID.randomUUID().toString();
                Operacao operacao = new Operacao(id, BigDecimal.valueOf(valor), referencia);

                Map<String, String> header = new HashMap<>();
                header.put("correlationId",UUID.randomUUID().toString());

                dispatcher.send(key, operacao, header);
            }
        }

        LocalDateTime fim = LocalDateTime.now();

        System.out.println("Tempo: " + Duration.between(inicio, fim).getSeconds());
    }
}
