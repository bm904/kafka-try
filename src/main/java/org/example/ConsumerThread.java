package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerThread {
    private final Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());
    private String serverAdress = "127.0.0.1:9092";
    private String groupId = "my-group-id";
    private String topic = "first_topic";
    private CountDownLatch latch = new CountDownLatch(1);

    private ConsumerThread(){
    }

    private void run()  {
        logger.info("================================");
        logger.info("==Creating the consumer thread==");
        Runnable consumerRunnable = new ConsumerRunnable(serverAdress, groupId, topic, latch);
        Thread consumerthread = new Thread(consumerRunnable);
        consumerthread.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Shutdown hook caught.");
            ((ConsumerRunnable) consumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited !");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application has been interrupted !", e);
        } finally {
            logger.info("Application is closing !");
        }
    }

    public static void main(String[] args) {
        new ConsumerThread().run();
    }

    public class ConsumerRunnable implements  Runnable{
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public  ConsumerRunnable(String bootstrapServer, String groupId, String topic, CountDownLatch latch){
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAdress);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            this.consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try{
                while(true){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> consumerRecord: records) {
                        logger.info("Key : {}", consumerRecord.key());
                        logger.info("Value : {}", consumerRecord.value());
                        logger.info("Partition : {}", consumerRecord.partition());
                        logger.info("Offset : {}", consumerRecord.offset());
                        logger.info("Timestamp : {}", consumerRecord.timestamp());
                    }
                }
            } catch(WakeupException e){
                logger.info("Received shutdown signal signal !");
            } finally {
                consumer.close();
                latch.countDown();
            }

        }

        public void shutdown(){
            consumer.wakeup();
        }

    }
}
