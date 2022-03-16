package br.com.alura.ecomerce;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String, String>(properties());                   // Kafkaproducer recebe propriedades(produtor)

        for (var i = 0; i < 100; i++) {                                                 //laço pra enviar mensagem 100 vezes

            var key = UUID.randomUUID().toString();                                 //nossa chave vai ser um id diferente de usuário
            var value = key + "8263263,1234";                                                //  a mensagem chave + (valor) que eu vou mandar
            //primeiro tópico
            var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", key, value);    // Mensagem criada -topico, chave e o valor
            Callback callback = (data, ex) -> {                                           //enviador de novo pedido
                if (ex != null) {
                    ex.printStackTrace();
                    return;
                }
                System.out.println(("sucesso enviado " + data.topic() + ":::partition" + data.partition() + "/ offset" + data.offset() + "/" + data.timestamp()));
            };
            var email = "Thank you for your order! We are processing your order!";
            //Segundo tópico
            var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, email);
            producer.send(record, callback).get();                                                                                  //enviar alguma coisa(mensagem, registro ou record) fica registrada no kafka pelo tempo que esta setado no server properties
            producer.send(emailRecord, callback).get();
        }
    }

    private static Properties properties() { // metodo estático que devolve um properties
        var propeties = new Properties(); // nossas propriedades
        propeties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); // onde esta conectando o kafka (chave e servidor/porta)
        propeties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());//Transformadores ou serializadores de strings para byts(classe)
        propeties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());//Transformadores ou serializadores de strings para byts(value= mensagem)
        return propeties;
    }
}
