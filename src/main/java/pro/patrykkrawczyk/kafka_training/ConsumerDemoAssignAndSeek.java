package pro.patrykkrawczyk.kafka_training;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class);

    private static final String SERVER_ADDRESS = "192.168.0.158:9092";
    private static final String GROUP_ID = "my-test-group";
    private static final String TOPIC = "test-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDRESS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        TopicPartition partition = new TopicPartition(TOPIC, 0);

        consumer.assign(Collections.singleton(partition));

        long offsetToReadFrom = 15L;
        consumer.seek(partition, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        int numberOfMessagesRead = 0;
        boolean keepOnReading = true;

        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> r : records) {
                ++numberOfMessagesRead;
                logger.info("Key: " + r.key() + " Value: " + r.value());
                logger.info("Partition: " + r.partition() + " Offset: " + r.offset());

                if (numberOfMessagesRead >= numberOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }
    }
}
