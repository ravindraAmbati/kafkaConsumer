package com.learn.kafkaConsumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learn.kafkaConsumer.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;

//@Component
@Slf4j
public class LibraryEventsConsumer {

    @Autowired
    LibraryEventService libraryEventService;

    @KafkaListener(topics = {"local-library-events", "default-library-events"})
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("Consumer Record: {}", consumerRecord);
        libraryEventService.processLibraryEvent(consumerRecord);
        log.info("successfully processed");
    }
}
