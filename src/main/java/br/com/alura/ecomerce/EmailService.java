package br.com.alura.ecomerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EmailService {
    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(properties());
        consumer.subscribe(Collections.singletonList("ECOMMERCE_SEND_EMAIL"));                                        //consumindo a mensagem de algum topico
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));                                  // perguntando ao consumer se tem mensagem por 100 milisecs
            if (!records.isEmpty()) {                                                                   //se os registros estão vazios
                System.out.println("Encontrei" + records.count() + "registros");
                for (var record : records) {
                    System.out.println("______________________________________________");
                    System.out.println("Send email");
                    System.out.println(record.key());
                    System.out.println(record.value());                     //valor da mensagem
                    System.out.println(record.partition());                 //particao q foi enviada
                    System.out.println(record.offset());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("Email sent");
                }
            }
        }
    }

    private static Properties properties() {
        var properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());        // transformar a chave de byte pra string
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, EmailService.class.getSimpleName());              //grupo que recebe todas as mensagens
        return properties;
    }

}
