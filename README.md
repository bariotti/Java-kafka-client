# java-kafka-client

Esse projeto trata-se de um simples exemplo de um Consumer e Producer Kafka em Java.

A versão utilizada do Kafka é 2.4.0.

Basicamente, baixei o Kafka na pagina da Apache (https://kafka.apache.org/downloads) e descompactei no diretório: C:\Kafka\kafka_2.12-2.4.0
Alterei os arquivos de configuração do Kafka e do Zookeeper, ajustando as variaveis dos LOGS, conforme abaixo:
Não ha necessidade de fazer a alteração abaixo, pois por padrão, os logs são armazenados em diretórios temporários, com isso toda ver que o Kafka/Zookeeper são reinicializados, os dados são perdidos.
Fazendo essa alteração, os dados se mantem e não são perdidos.

Mantive as portas padrões, 9092 para o KAFKA e 2181 para o ZOOKEEPER. As portas também são parametrizaveis nos arquivos de configuração.

Obs.: O projeto é para exemplificar o desenvolivmento de um Consumer e Producer em JAVA. Em um ambiente produtivo/controlado, diversas outras responsabilidades devem ser abordadas.

---KAFKA
Arquivo de configuração: C:\Kafka\kafka_2.12-2.4.0\config\server.properties
Variavel: log.dirs
Valor da Variavel: C:/Kafka/kafka_2.12-2.4.0/data/kafka

---ZOOKEEPER
Arquivo de configuração: C:\Kafka\kafka_2.12-2.4.0\config\zookeeper.properties
Variavel: dataDir
Valor da Variavel: C:/Kafka/kafka_2.12-2.4.0/data/zookeeper

Comandos utilizados via terminal:

---Iniciar o serviço do ZOOKEEPER:
kafka-server-start.bat .\config\server.properties

---Iniciar o serviço do KAFKA:
kafka-server-start.bat .\config\server.properties

---Criar um tópico (nome do topico: operação, quantidade de partições: 2):
 kafka-topics.bat --bootstrap-server localhost:9092 --create --topic operacao --partitions 2
 
 ---Listar os topicos:
  kafka-topics.bat --bootstrap-server localhost:9092 --list
  
  ---Iniciar um Console Consumer (topico: operacao, consumer group: g1):
  kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic operacao --group g1
  
  ---Iniciar um Console Producer (topico: operacao):
  kafka-console-producer.bat --broker-list localhost:9092 -topic operacao
