package com.learn.kafkaConsumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learn.kafkaConsumer.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@Profile("ack")
public class LibraryEventsConsumerManualAcknowledgment implements AcknowledgingMessageListener<Integer, String> {

    @Autowired
    LibraryEventService libraryEventService;

    private static long messageCount = 0;

    @Override
    @KafkaListener(topics = {"local-library-events", "default-library-events"})
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
        log.info("Message Count: {}", ++messageCount);
        log.info("Consumer Record: {}", consumerRecord);
        try {
            libraryEventService.processLibraryEvent(consumerRecord);
        } catch (JsonProcessingException e) {
            log.error(e.getMessage());
        }
        acknowledgment.acknowledge();
        log.info("Acknowledged");
    }
}
