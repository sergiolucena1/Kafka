package br.com.alura.ecomerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {
    public static void main(String[] args) {
        var emailService = new EmailService();
        var service = new KafkaService(EmailService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER",
                emailService::parse);
        service.run();
    }

    private void parse(ConsumerRecord<String, String> record) {              //função que recebe um consumer record
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
