package com.learn.kafkaConsumer.consumer;

import com.learn.kafkaConsumer.service.LibraryEventService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventsConsumerManualAcknowledgment implements AcknowledgingMessageListener<Integer, String> {

    @Autowired
    LibraryEventService libraryEventService;

    @SneakyThrows
    @Override
    @KafkaListener(topics = {"local-library-events", "default-library-events"})
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
        log.info("Consumer Record: {}", consumerRecord);
        libraryEventService.processLibraryEvent(consumerRecord);
        acknowledgment.acknowledge();
        log.error("Acknowledged");
    }
}
