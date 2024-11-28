package tp3;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Scanner;

public class ProducerDemo {
    public static void main(String[] args) {
        // Configurações do producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "broker1:9092"); // Altere se necessário
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Criação do producer
        try (Producer<String, String> producer = new KafkaProducer<>(props);
                Scanner scanner = new Scanner(System.in)) {

            System.out.println("Digite suas mensagens para enviar ao tópico Kafka. Digite 'sair' para encerrar.");

            while (true) {
                System.out.print("Digite a mensagem: ");
                String value = scanner.nextLine();

                // Finaliza se o usuário digitar "sair"
                if ("sair".equalsIgnoreCase(value)) {
                    System.out.println("Encerrando o producer...");
                    break;
                }

                // Envio da mensagem com uma chave opcional
                System.out.print("Digite a chave (ou pressione Enter para nenhuma chave): ");
                String key = scanner.nextLine();

                // Enviar a mensagem
                ProducerRecord<String, String> record = (key.isEmpty())
                        ? new ProducerRecord<>("test-topic", value)
                        : new ProducerRecord<>("test-topic", key, value);

                producer.send(record);
                System.out.println("Enviado: " + (key.isEmpty() ? "Sem chave" : key) + " -> " + value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}