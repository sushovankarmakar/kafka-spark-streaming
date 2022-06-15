package kafka;

public class KafkaRunner {

    public static void main(String[] args) {
        CSVKafkaProducer kafkaProducer = new CSVKafkaProducer();
        kafkaProducer.publishMessages();
        System.out.println("Producing job completed.");
    }
}