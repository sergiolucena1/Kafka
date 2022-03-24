package br.com.alura.ecomerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;


public class FraudDetectorService {

    public static void main(String[] args) {
        var fraudService = new FraudDetectorService();
        var service = new KafkaService(FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudService::parse);
        service.run();
    }                                                                                           //consumindo a mensagem de algum topico

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("______________________________________________");
        System.out.println("Processing new order, cheking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());                     //valor da mensagem
        System.out.println(record.partition());                 //particao q foi enviada
        System.out.println(record.offset());                    //localização da mensagem da partição
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Order processed");
    }


}
