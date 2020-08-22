package com.learn.kafkaConsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafkaConsumer.entity.Book;
import com.learn.kafkaConsumer.entity.LibraryEvent;
import com.learn.kafkaConsumer.entity.LibraryEventType;
import com.learn.kafkaConsumer.repository.jpa.BooKJpaRepo;
import com.learn.kafkaConsumer.repository.jpa.LibraryEventJpaRepo;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventService {

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    LibraryEventJpaRepo libraryEventJpaRepo;

    @Autowired
    BooKJpaRepo booKJpaRepo;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("consumerRecord: {}", consumerRecord);
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        processLibraryEvent(libraryEvent);
    }

    private void processLibraryEvent(LibraryEvent libraryEvent) {
        validate(libraryEvent);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        log.info("book: {}", libraryEvent.getBook());
        libraryEventJpaRepo.save(libraryEvent);
        libraryEventJpaRepo.findAll().forEach(
                event -> log.info("libraryEvent: {}", event)
        );
        booKJpaRepo.findAll().forEach(
                book -> log.info("book: {}", book)
        );
    }

    private void validate(LibraryEvent libraryEvent) {
        Integer libraryEventId = libraryEvent.getLibraryEventId();
        LibraryEventType libraryEventType = libraryEvent.getLibraryEventType();
        Integer bookId = libraryEvent.getBook().getBookId();
        if (null != libraryEventId && libraryEventId == 0) {
            throw new RecoverableDataAccessException("for testing purpose only");
        }
        if (LibraryEventType.NEW.equals(libraryEventType)) {
            if (null != libraryEventId) {
                throw new IllegalArgumentException("LibraryEventId should be null for new library events");
            }
            if (null != bookId) {
                throw new IllegalArgumentException("BookId should be null for new library events");
            }
        } else if (LibraryEventType.UPDATE.equals(libraryEventType)) {
            if (null == libraryEventId) {
                throw new IllegalArgumentException("LibraryEventId should not be null for update library events");
            } else if (null == bookId) {
                throw new IllegalArgumentException("BookId should not be null for update library events");
            } else {
                Optional<Book> optionalBook = booKJpaRepo.findById(bookId);
                if (optionalBook.isEmpty()) {
                    throw new IllegalArgumentException("Provided Book Id is missing");
                }
            }
        }
    }

}
