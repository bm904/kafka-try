package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerAssignSeek {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerAssignSeek.class);
    private static String serverAdress = "127.0.0.1:9092";
    private static String topic = "first_topic";

    public static void main(String[] args) {
        /* Propriétés du Consumer */
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAdress);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        /* Créer le Consumer */
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        TopicPartition topicPartition = new TopicPartition(topic, 0);
        Long offset = 15L;
        consumer.assign(Arrays.asList(topicPartition));

        consumer.seek(topicPartition, offset);

        int numberMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberMessagesAlreadyRead = 0;

        while(keepOnReading){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> consumerRecord: records) {
                logger.info("Key : {}", consumerRecord.key());
                logger.info("Value : {}", consumerRecord.value());
                logger.info("Partition : {}", consumerRecord.partition());
                logger.info("Offset : {}", consumerRecord.offset());
                logger.info("Timestamp : {}", consumerRecord.timestamp());
                if(numberMessagesAlreadyRead > numberMessagesToRead){
                    keepOnReading = false;
                    break;
                }
            }
        }
        logger.info("Exiting the application.");

    }
}
