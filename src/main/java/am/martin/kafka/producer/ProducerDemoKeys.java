package am.martin.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers = "127.0.0.1:9092";

        // create producer properties
        var properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        var producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            var topic = "first_topic";
            var value = "Hello World - " + i;
            var key = "id_" + i;

            // create a producer record
            var record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: " + key); // log the key

            // send data - asynchronous
            producer.send(record, (recordMetadata, e) -> {
                // executes every time a record is successfully sent or an exception is thrown
                if (e == null) {
                    // the record was successfully sent
                    logger.info("Received new metadata.\n\t" +
                            "Topic: " + recordMetadata.topic() + "\n\t" +
                            "Partition: " + recordMetadata.partition() + "\n\t" +
                            "Offset: " + recordMetadata.offset() + "\n\t" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
//                        e.getStackTrace();
                    logger.error("Error while producing", e);
                }
            }).get(); // block the .send() to make it synchronous - don't do this in production!
        }

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();
    }
}
