/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.kafka.consumer.source;

import com.mycompany.kafka.test.cluster.Config;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;

/**
 *
 * @author DuyenThai
 */
public class KafKaConsumer {

    private static final Logger LOGGER = Logger.getLogger(KafKaConsumer.class);

    public KafKaConsumer() {
    }

    public Consumer<Long, String> createConsumer(String topic) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_SERVERS);
        //if this field is set with a predefined group id, depend on the auto_offset_config, the consumer may be just available to read the latest record
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Config.CLIENT_CONSUMER);
        //this is optional. If there are more than one consumer in the group, it is recommended that this field should be set
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
        LOGGER.info("Initialized consumer");

        consumer.subscribe(Collections.singleton(topic));
        LOGGER.info("Subcribed to target topic");
        return consumer;
    }
}
