package org.blitzscaling;

import com.example.Customer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaAvroConsumerv1 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "0.0.0.0:9092");
        properties.setProperty("group.id", "avro-consumer");
        properties.setProperty("auto.commit.enable", "false");
        properties.setProperty("auto.offset.reset", "earliest");

        // avro set-up
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
        properties.setProperty("specific.avro.reader", "true");

        KafkaConsumer<String, Customer> consumer = new KafkaConsumer<>(properties);
        String topic = "customer-avro";
        consumer.subscribe(Collections.singleton(topic));
        System.out.println("Waiting for data...");

        while (true) {
            ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, Customer> record : records) {
                Customer customer = record.value();
                System.out.println(customer);
                System.out.println(record.partition());
                System.out.println(record.offset());
            }

            consumer.commitSync();
        }
    }
}
