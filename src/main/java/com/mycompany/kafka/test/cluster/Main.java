/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.kafka.test.cluster;

import org.apache.log4j.BasicConfigurator;

/**
 *
 * @author DuyenThai
 */
public class Main {

    public static void main(String[] args) {
        BasicConfigurator.configure();
        ProcessWorker worker = new ProcessWorker();
        worker.processKafka();
    }
}
