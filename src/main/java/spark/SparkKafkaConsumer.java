package spark;

import kafka.serializer.StringDecoder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * https://www.youtube.com/watch?v=UcWi3-FODjs
 * https://stackoverflow.com/questions/57956511/java-io-notserializableexception-org-apache-kafka-clients-consumer-consumerreco
 * https://github.com/PacktPublishing/-Data-Stream-Development-with-Apache-Spark-Kafka-and-Spring-Boot/blob/master/AnalysisTier/SparkKafkaToDStreamToMongoDB/src/main/java/com/rsvps/StreamingRsvpsDStream.java
 * https://github.com/wangyangjun/StreamBench/blob/master/StreamBench/spark/pom.xml
 * https://github.com/mukulbansal93/tutorials/blob/master/src/main/java/info/mb/tutorial/spark/stream/JSONProcessingExample.java
 *
 * https://www.google.com/search?q=javapairinputdstream+example+github&sxsrf=ALiCzsbSe78Ws88Jxa_3Y6_nRXl7Sx8dgw%3A1655238183429&ei=J-6oYurlGZja4-EP5paiqAo&oq=javapairinputdstream+example+g&gs_lcp=Cgdnd3Mtd2l6EAMYADIFCCEQoAEyBQghEKABMgUIIRCgAToICAAQsAMQogRKBAhBGAFKBAhGGABQFVjoDWCSG2gBcAB4AIABwAGIAekCkgEDMC4ymAEAoAEByAEEwAEB&sclient=gws-wiz
 * https://spark.apache.org/docs/latest/streaming-programming-guide.html#discretized-streams-dstreams
 * https://www.youtube.com/results?search_query=kafka+java+tutorial
 */
public class SparkKafkaConsumer {

    private static final Map<String, Object> KAFKA_CONSUMER_PROPERTIES;

    private static final String KAFKA_BROKERS = "localhost:9092";
    private static final String KAFKA_GROUP = "demoGroup";
    private static final String KAFKA_TOPIC = "demo";
    private static final Collection<String> TOPICS = Collections.singletonList(KAFKA_TOPIC);

    static {
        Map<String, Object> kafkaProperties = new HashMap<>();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_GROUP);

        KAFKA_CONSUMER_PROPERTIES = Collections.unmodifiableMap(kafkaProperties);
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Spark streaming started now");

        SparkConf conf = new SparkConf()
                .setAppName("spark-kafka-app")
                .setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext, new Duration(10000));

        /*Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        Set<String> topics = Collections.singleton("demo");

        JavaPairInputDStream<String, String> directStream = KafkaUtils.createDirectStream(
                javaStreamingContext, String.class, String.class,
                StringDecoder.class, StringDecoder.class, kafkaParams, topics);*/

        final JavaInputDStream<ConsumerRecord<String, String>> directStream =
                KafkaUtils.createDirectStream(
                        javaStreamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(TOPICS, KAFKA_CONSUMER_PROPERTIES)
                );

        //List<String> allRecords = new ArrayList<>();
        final String COMMA = ",";
        final String NEW_LINE = "\n";

        directStream.foreachRDD(rdd -> {
            System.out.println("New data arrived " + rdd.partitions().size() + " Partitions and " + rdd.count() + " records.");

            if (rdd.count() > 0) {
                BufferedWriter writer = new BufferedWriter(new FileWriter("master_dataset.csv", false));

                // https://stackoverflow.com/questions/57956511/java-io-notserializableexception-org-apache-kafka-clients-consumer-consumerreco
                JavaPairRDD<String, String> outStream = rdd.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
                outStream.collect().forEach(record -> {
                    System.out.println(record._1 + " " + record._2);
                });

                outStream.collect().forEach(rawRecord -> {
                    System.out.println("***************************************");
                    System.out.println(rawRecord._2);
                    String record = rawRecord._2();
                    StringTokenizer st = new StringTokenizer(record, ",");

                    StringBuilder line = new StringBuilder();
                    while (st.hasMoreTokens()) {
                        String step = st.nextToken(); // Maps a unit of time in the real world. In this case 1 step is 1 hour of time.
                        String type = st.nextToken(); // CASH-IN,CASH-OUT, DEBIT, PAYMENT and TRANSFER
                        String amount = st.nextToken(); //amount of the transaction in local currency
                        String nameOrig = st.nextToken(); //  customerID who started the transaction
                        String oldBalanceOrg = st.nextToken(); // initial balance before the transaction
                        String newBalanceOrig = st.nextToken(); // customer's balance after the transaction.
                        String nameDest = st.nextToken(); // recipient ID of the transaction.
                        String oldBalanceDest = st.nextToken(); // initial recipient balance before the transaction.
                        String newBalanceDest = st.nextToken(); // recipient's balance after the transaction.
                        String isFraud = st.nextToken(); // identifies a fraudulent transaction (1) and non fraudulent (0)
                        String isFlaggedFraud = st.nextToken(); // flags illegal attempts to transfer more than 200.000 in a single transaction.

                        // Keep only interested column in Master Data set.
                        line.append(step).append(COMMA)
                                .append(type).append(COMMA)
                                .append(amount).append(COMMA)
                                .append(oldBalanceOrg).append(COMMA)
                                .append(newBalanceOrig).append(COMMA)
                                .append(oldBalanceDest).append(COMMA)
                                .append(newBalanceDest).append(COMMA)
                                .append(isFraud).append(NEW_LINE);
                        //allRecords.add(line.toString());

                        try {
                            writer.write(line.toString());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
                System.out.println("Master dataset has been created.");
                writer.close();
            }
        });

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}