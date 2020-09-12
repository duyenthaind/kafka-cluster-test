/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.kafka.test.cluster;

import com.mycompany.kafka.consumer.source.KafKaConsumer;
import com.mycompany.kafka.producer.source.KafKaProducer;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.log4j.Logger;

/**
 *
 * @author DuyenThai
 */
public class ProcessWorker {

    private static final Logger LOGGER = Logger.getLogger(ProcessWorker.class);
    private static final String TOPIC = Config.TOPIC;
    private KafKaProducer producerManager;
    private Producer<Long, String> producer = null;
    private Consumer<Long, String> consumer = null;
    private ListenThread consumerWorker;
    private ProducerThread producerWorker;

    public ProcessWorker() {
    }

    public void processKafka() {
        producerManager = new KafKaProducer();
        producer = producerManager.createProducer(TOPIC);
        consumer = new KafKaConsumer().createConsumer(TOPIC);
        Scanner scanner = new Scanner(System.in);
        consumerWorker = new ListenThread();
        producerWorker = new ProducerThread();
        try {
            consumerWorker.start();
            producerWorker.start();
            while (producerWorker.isAlive()) {
            }
            endProcess();
        } catch (Exception ex) {
            LOGGER.error(ex, ex);
            consumerWorker.closeThread();
            producer.close();
            System.exit(1);
        }

    }

    private void endProcess() {
        consumerWorker.closeThread();
        producer.close();
        LOGGER.warn("Ending process!!!");
        System.exit(0);
    }

    class ListenThread extends Thread {

        private final AtomicBoolean running = new AtomicBoolean(true);

        @Override
        public void run() {
            while (running.get()) {
                final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
                if (consumerRecords.count() > 0) {
                    LOGGER.info("Size: " + consumerRecords.count());
                }

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
            LOGGER.info("Complete process");
        }

        public void closeThread() {
            running.set(false);
        }
    }

    class ProducerThread extends Thread {

        @Override
        public void run() {
            int seq = 0;
            long start = System.currentTimeMillis();
            while (seq < 50) {
                long now = System.currentTimeMillis();
                if ((now - start) % 10000 == 0) {
                    producerManager.runProducer(1, "Test worker on cluster kafka" + ++seq + " " + now, producer);
                }
            }
        }

    }

}
