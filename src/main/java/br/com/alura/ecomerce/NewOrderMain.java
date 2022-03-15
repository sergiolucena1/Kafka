package br.com.alura.ecomerce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String, String>(properties());                   // Kafkaproducer recebe propriedades(produtor)
        var value = "13212232,8263263,233468343473";                                     //  a mensagem(valor) que eu vou mandar
        var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value, value);    // Mensagem criada -topico, chave e o valor
        producer.send(record, (data, ex) -> {                                           //enviador de novo pedido
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println(("sucesso enviado " + data.topic() + ":::partition" + data.partition() + "/ offset" + data.offset() + "/" + data.timestamp()));
        }).get();                                                                           //enviar alguma coisa(mensagem, registro ou record) fica registrada no kafka pelo tempo que esta setado no server properties
    }

    private static Properties properties() { // metodo estático que devolve um properties
        var propeties = new Properties(); // nossas propriedades
        propeties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); // onde esta conectando o kafka (chave e servidor/porta)
        propeties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());//Transformadores ou serializadores de strings para byts(classe)
        propeties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());//Transformadores ou serializadores de strings para byts(value= mensagem)
        return propeties;
    }
}
