package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * https://www.youtube.com/watch?v=UcWi3-FODjs
 * https://www.youtube.com/watch?v=bwXWIx5dZjw
 * https://www.youtube.com/watch?v=5AENxG_Bvns
 */
public class CSVKafkaProducer {
    private static final String kafkaBrokerEndPoint = "localhost:9092";
    private static final String kafkaTopic = "demo";
    private static final String csvFile = "test.csv";

    private final Producer<String, String> producer;

    public CSVKafkaProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerEndPoint);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaCSVProducer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(properties);
    }

    protected void publishMessages() {
        try {
            URI uri = Objects.requireNonNull(getClass().getClassLoader().getResource(csvFile)).toURI();
            Stream<String> lines = Files.lines(Paths.get(uri));
            lines.forEach(line -> {
                System.out.println(line);

                ProducerRecord<String, String> record = new ProducerRecord<>(
                        kafkaTopic, UUID.randomUUID().toString(), line
                );

                producer.send(record, (metadata, exception) -> {
                    if (metadata != null) {
                        System.out.println("CSV Data : " + record.key() + " | " + record.value());
                    } else {
                        System.out.println("Error while sending record " + record.value());
                    }
                });
            });
            producer.close();
            lines.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}