package com.kafka.customdeserializers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

public class OrderConsumer {
    public static void main(String[] args) {

        AtomicReference<MessageDto> msgCons = new AtomicReference<>();

        KafkaConsumer<String, MessageDto> consumer = createKafkaConsumer();
        consumer.subscribe(Arrays.asList("TOPIC"));

        ConsumerRecords<String, MessageDto> records = consumer.poll(Duration.ofSeconds(1));
        records.forEach(record -> {
            msgCons.set(record.value());
            System.out.println("Message received " + record.value());
        });

        consumer.close();
//        Properties props = new Properties();
//        props.setProperty("bootstrap.servers", "localhost:9092");
////        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
////        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
//        props.setProperty("key.deserializer", StringDeserializer.class.getName());
//        props.setProperty("value.deserializer", OrderDeserializer.class.getName());
//        props.setProperty("group.id", "OrderCSGroup");
//
//
//        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
//        consumer.subscribe(Collections.singletonList("OrderCSTopic"));
//
//        ConsumerRecords<String, Order> records = consumer.poll(Duration.ofSeconds(2000));
//        for (ConsumerRecord<String, Order> record : records) {
////            System.out.println("Product name " + record.key());
////            System.out.println("Quantity " + record.value());
//            String consumerName = record.key();
//            Order order = record.value();
//            System.out.println("Customer name: " + order.getCustomerName());
//            System.out.println("Product: " + order.getProduct());
//            System.out.println("Quantity: " + order.getQuantity());
//        }
//        consumer.close();
//
////        } catch (Exception e) {
////            e.printStackTrace();
////        } finally {
////            consumer.close();

    }

    private static KafkaConsumer<String, MessageDto> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "123");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "CONSUMER_GROUP_ID");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.kafka.customdeserializers.CustomDeserializer");

        return new KafkaConsumer<>(props);
    }
}
