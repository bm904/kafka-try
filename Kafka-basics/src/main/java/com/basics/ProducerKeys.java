package com.basics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerKeys {
    private static final Logger logger = LoggerFactory.getLogger(ProducerKeys.class);
    private static String serverAdress = "127.0.0.1:9092";
    
    public static void main(String[] args) {
        /* Propriétés du Producer */
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAdress);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /* Créer le Producer */
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0; i<10; i++ ){
            String topic = "first_topic";
            String value = "Hellow guys! "+ Integer.toString(i);
            String key = "id_" +  Integer.toString(i);
            logger.info("Key: {}", key);
            /* Créer un Producer Record */
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            /* Envoyer les données */
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        logger.info("New metadata received");
                        logger.info("Topic : {}", recordMetadata.topic());
                        logger.info("Partition : {}", recordMetadata.partition());
                        logger.info("Offset : {}", recordMetadata.offset());
                        logger.info("Timestamp : {}", recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });
        }


        /* Flusher les données et fermer */
        producer.flush();
        producer.close();
    }
}
