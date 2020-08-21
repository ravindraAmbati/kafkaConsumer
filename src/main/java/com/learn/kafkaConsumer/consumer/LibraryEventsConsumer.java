package com.learn.kafkaConsumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learn.kafkaConsumer.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@Profile("nonAck")
public class LibraryEventsConsumer {

    @Autowired
    LibraryEventService libraryEventService;

    private static long messageCount = 0;

    @KafkaListener(topics = {"local-library-events", "default-library-events"})
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("Message Count: {}", ++messageCount);
        log.info("Consumer Record: {}", consumerRecord);
        libraryEventService.processLibraryEvent(consumerRecord);
        log.info("successfully processed");
    }
}
