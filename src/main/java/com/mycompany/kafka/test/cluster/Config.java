/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.kafka.test.cluster;

/**
 *
 * @author DuyenThai
 */
public class Config {
//    public static final String TOPIC = "multi-brokers";

    public static final String TOPIC = "multi-broker2";
    public static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";
    public static final String CLIENT_PRODUCER = "KafkaProducer";
    public static final String CLIENT_CONSUMER = "KafkaConsumer";
    public static final String PRODUCER_BATCH_SIZE = "16384";
    public static final String PRODUCER_LINGER_MS = "0";
    public static final String RECONNECT_TIME_MS = "50";
}
