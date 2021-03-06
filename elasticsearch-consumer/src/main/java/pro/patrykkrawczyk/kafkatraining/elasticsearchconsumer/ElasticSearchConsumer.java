package pro.patrykkrawczyk.kafkatraining.elasticsearchconsumer;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);
    private static final JsonParser jsonParser = new JsonParser();

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createClient();
        KafkaConsumer<String, String> consumer = createKafkaConsumer("twitter-tweets");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            BulkRequest bulkRequest = new BulkRequest();

            int recordCount = records.count();
            logger.info("Received " + recordCount + " records");
            for (ConsumerRecord<String, String> r : records) {
                try {
                    String id = extractIdFromTweet(r.value());
                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id)
                            .source(r.value(), XContentType.JSON);

                    bulkRequest.add(indexRequest);
                } catch (Exception e) {
                    logger.info("skipping bad data: " + r.value());
                }
            }

            if (recordCount > 0) {
                BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Syncing offsets...");
                consumer.commitSync();
                logger.info("Offsets have been committed");
            }
        }

//        client.close();
    }

    private static RestHighLevelClient createClient() {
        String hostname = System.getenv("BONSAI_IO_HOSTNAME");
        String username = System.getenv("BONSAI_IO_USERNAME");
        String password = System.getenv("BONSAI_IO_PASSWORD");

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient
                .builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(h -> h.setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(builder);
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(String topic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.158:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-demo-elasticsearch");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(topic));
        return consumer;
    }

    private static String extractIdFromTweet(String json) {
        return jsonParser.parse(json)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
}
