# kafka-cluster-test
A project using kafka-cluster to distribute and get message

Requirements:
- Kafka apache: 2.5.0
- Server: localhost with 3 ports available
- Zookeeper: embedded with kafka apache
- Java 8 or greater

Install:
Config servers kafka
  - Server 1: change the config in file server.properties
     + listen: 
     + log: kafka-logs
     + offset: 3
     + nodes: 3
  - Server 2: copy the file server.properties then change the following configuration
      + listen: plaintext:9093
      + log: kafka-logs2
      + offset: 3
      + nodes: 3
  - Server 2: copy the file server.properties then change the following configuration
      + listen: plaintext:9094
      + log: kafka-logs2
      + offset: 3
      + nodes: 3
