/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.kafka.test.cluster;

import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 *
 * @author DuyenThai
 */
public class TestConsumer {

    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_SERVERS);
        //this field will generate a new group consumer, this group will get all the message in the cluster kafka
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Config.CLIENT_CONSUMER + System.currentTimeMillis());
        //this is optional, it is recommended that it should be filled if there are more than one consumer in the kafka cluster
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        //enable the consumer to commit the consumer offset in the background
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //interval between two time commit config
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "500");
        //Deserializer for key of the record
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        //Deserializer for value of the record
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //set the time to reconnect after the system is broken
        props.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, Config.RECONNECT_TIME_MS);
        //increase to avoid time out for requesting to fetch record
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "60000");
        //advoid zookeeper session from being timed out
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000");
        //decrease the time consumer send notification to brokers that it is still alive, advoiding time out error
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
        //decrease the time interval between to time poll continously
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "50");
        //reset the offset to the earliest offset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //decrease the max poll to increase the times poll function of consumer is called, advoiding time out error
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

        final Consumer<Long, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(Config.TOPIC));
        consumer.seekToBeginning(consumer.assignment());
//        long start = System.currentTimeMillis();
//        while (System.currentTimeMillis() - start <= 100000) {
//            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
//
//            for (ConsumerRecord<Long, String> record : consumerRecords) {
//                System.out.printf("Get record from producer: (%d, %s, %d, %d)\n",
//                        record.key(), record.value(),
//                        record.partition(), record.offset());
//            }
//            consumer.commitAsync();
//        }
//        consumer.unsubscribe();
//        consumer.close();
        NewRuntime test = new NewRuntime(consumer);
        test.start();

    }

    static class NewRuntime extends Thread {

        private Consumer<Long, String> consumer;

        public NewRuntime(Consumer<Long, String> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void run() {
            long start = System.currentTimeMillis();
            long limit = 100000;
            while (System.currentTimeMillis() - start <= limit) {
                final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
                for (ConsumerRecord<Long, String> record : consumerRecords) {
                    System.out.printf("Get record from producer: (%d, %s, %d, %d)\n",
                            record.key(), record.value(),
                            record.partition(), record.offset());
                    System.out.println("");
                }
                consumer.commitAsync();
            }
            consumer.unsubscribe();
            consumer.close();
            System.out.println("DONE");
        }

    }
}
