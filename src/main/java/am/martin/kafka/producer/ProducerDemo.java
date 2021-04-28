package am.martin.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        var bootstrapServers = "127.0.0.1:9092";

        //create producer properties
        var properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        var producer = new KafkaProducer<String, String>(properties);

        //create a producer record
        var record = new ProducerRecord<String, String>("first_topic", "hello world");

        //send data - asynchronous
        producer.send(record);

        //flush data
        producer.flush();

        //flush and close producer
        producer.close();

    }
}
