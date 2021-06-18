package org.blitzscaling;

import com.example.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaAvroProducerv1 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "0.0.0.0:9092");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");
        // avro set-up
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

        Producer<String, Customer> kafkaProducer = new KafkaProducer<String, Customer>(properties);
        String topic = "customer-avro";

        Customer customer = Customer.newBuilder()
                .setAge(29)
                .setAutomatedEmail(true)
                .setFirstName("jianfeng")
                .setLastName("guo")
                .setHeight(175)
                .setWeight(60f)
                .build();

        ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(topic, customer);
        kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
            if (e == null) {
                System.out.println("success");
                System.out.println(recordMetadata.toString());
            } else {
                e.printStackTrace();
            }
        });
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
