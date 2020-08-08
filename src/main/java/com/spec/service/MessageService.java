package com.spec.service;

import com.spec.KafkaConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class MessageService {

    private final static Logger LOGGER = Logger.getLogger(MessageService.class.getName());

    public void processMessage() {

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
            KafkaConfig.getConfig())) {
            while (true) {
                consumer.subscribe(Collections.singleton("mymessages"));
                ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> consumerRecord : poll) {
                    LOGGER.info(consumerRecord.value());
                }
                TimeUnit.SECONDS.sleep(2);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
