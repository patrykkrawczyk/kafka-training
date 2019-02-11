package pro.patrykkrawczyk.kafka.training;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithCallbackAndKey {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallbackAndKey.class);

    private static final String SERVER_ADDRESS = "192.168.0.158:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDRESS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "test-topic";

        for (int i = 0; i < 10; ++i) {
            String key = "id_" + i;
            String value = "message_" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            logger.info("Key: " + key);

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata m, Exception e) {
                    if (e != null) {
                        logger.error("Error while producing", e);
                        return;
                    }

                    String msg =
                            String.format("Received new metadata:\nTopic: %s\nPartition: %s\nOffset: %s\nTimestamp: %s",
                                    m.topic(), m.partition(), m.offset(), m.timestamp());

                    System.out.println(msg);
                }
            }).get(); // Temporary, only to see key working, dont use at production!
        }

        producer.close();
    }
}
