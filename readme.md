How to Setup Kafka and zoo-keeper
----------------------------------

1. Download and Setup Java 8 JDK

2. Download the Kafka binaries from https://kafka.apache.org/downloads

3. Extract Kafka at the root of C:\

4. Setup Kafka bins in the **Environment variables** section by editing **Path**

5. Try Kafka commands using **kafka-topics.bat** (for example)

6. Create folder 'data' in C:\kafka_2.12-2.7.0\data, and then create two sub-folders 'C:\kafka_2.12-2.7.0\data\kafka' and 'C:\kafka_2.12-2.7.0\data\zookeeper'. These folders hold the logs of zoo keeper and kafka

7. Edit Zookeeper & Kafka configs using NotePad++ https://notepad-plus-plus.org/download/

	zookeeper.properties: dataDir=C:/kafka_2.12-2.0.0/data/zookeeper (yes the slashes are inversed)

	server.properties: log.dirs=C:/kafka_2.12-2.0.0/data/kafka (yes the slashes are inversed)


Starting ZooKeeper and Kafka
----

8. Start Zookeeper in one command line: zookeeper-server-start.bat config\zookeeper.properties

9. Start Kafka in another command line: kafka-server-start.bat config\server.properties