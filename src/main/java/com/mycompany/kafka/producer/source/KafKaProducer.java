/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.kafka.producer.source;

import com.mycompany.kafka.test.cluster.Config;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

/**
 *
 * @author DuyenThai
 */
public class KafKaProducer {

    private static final Logger LOGGER = Logger.getLogger(KafKaProducer.class);

    public KafKaProducer() {
    }

    public Producer<Long, String> createProducer(String topic) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_SERVERS);
        //generate a new producer with the predefined client_id
        props.put(ProducerConfig.CLIENT_ID_CONFIG, Config.CLIENT_PRODUCER);
        //encode and serializer for key of the reord
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        //encode and serializer for the value of the record
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //set the time to reconnect after the system is broken
        props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, Config.RECONNECT_TIME_MS);
        //wait 5ms to reduce requests, in 5ms all the requests will be sent as a batch if they arrives at the nearly time 5ms
        props.put(ProducerConfig.LINGER_MS_CONFIG, Config.PRODUCER_LINGER_MS);
        //set the batch size for producer, in kb
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, Config.PRODUCER_BATCH_SIZE);
        //the records must be acknowledged by all the brokers, advoid missing data
//        props.put(ProducerConfig.ACKS_CONFIG, "-1");
        LOGGER.info("Initalized new producer");
        return new KafkaProducer<>(props);
    }

    public void runProducer(final int sendMessageCount, String msg, Producer<Long, String> producer) {
        long time = System.currentTimeMillis();
        try {
            for (long index = time; index < time + sendMessageCount; index++) {
                final ProducerRecord<Long, String> record
                        = new ProducerRecord<>(Config.TOPIC, index, msg);

                RecordMetadata metadata = producer.send(record).get();

                long executeTime = System.currentTimeMillis() - time;
                System.out.printf("sent record (key=%s value=%s)"
                        + "meta(partition=%d, offset=%d) time=%d\n",
                        record.key(), record.value(), metadata.partition(),
                        metadata.offset(), executeTime);
            }
        } catch (InterruptedException | ExecutionException ex) {
            LOGGER.error(ex, ex);
        } finally {
            producer.flush();
        }
    }

}
