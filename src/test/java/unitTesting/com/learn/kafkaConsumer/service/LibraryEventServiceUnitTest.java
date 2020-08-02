package com.learn.kafkaConsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafkaConsumer.entity.Book;
import com.learn.kafkaConsumer.entity.LibraryEvent;
import com.learn.kafkaConsumer.entity.LibraryEventType;
import com.learn.kafkaConsumer.repository.jpa.BooKJpaRepo;
import com.learn.kafkaConsumer.repository.jpa.LibraryEventJpaRepo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LibraryEventServiceUnitTest {

    ObjectMapper objectMapper;

    @Mock
    ObjectMapper objectMapperMock;

    @Mock
    LibraryEventJpaRepo libraryEventJpaRepo;

    @Mock
    BooKJpaRepo booKJpaRepo;

    @InjectMocks
    LibraryEventService testClass;

    private ConsumerRecord<Integer, String> consumerRecord;
    private LibraryEvent libraryEvent;
    private Book book;
    private String kafkaTopic;

    @BeforeEach
    void setUp() throws JsonProcessingException {
        objectMapper = new ObjectMapper();
        kafkaTopic = "default-library-events";
    }

    @AfterEach
    void tearDown() {
        objectMapper = null;
        kafkaTopic = null;
        book = null;
        libraryEvent = null;
        consumerRecord = null;
    }

    @Test
    void processNewLibraryEvent_libraryEventIdNotNull() throws JsonProcessingException {
        book = Book.builder()
                .bookId(123)
                .name("aName")
                .author("anAuthor")
                .build();
        libraryEvent = LibraryEvent.builder()
                .libraryEventId(123)
                .libraryEventType(LibraryEventType.NEW)
                .book(book)
                .build();
        consumerRecord = new ConsumerRecord<>(kafkaTopic, 3, 0, libraryEvent.getLibraryEventId(), objectMapper.writeValueAsString(libraryEvent));
        when(objectMapperMock.readValue(consumerRecord.value(), LibraryEvent.class)).thenReturn(libraryEvent);
        assertThrows(IllegalArgumentException.class, () -> testClass.processLibraryEvent(consumerRecord), () -> "LibraryEventId should be null for new library events");
    }

    @Test
    void processNewLibraryEvent_bookIdNotNull() throws JsonProcessingException {
        book = Book.builder()
                .bookId(123)
                .name("aName")
                .author("anAuthor")
                .build();
        libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .libraryEventType(LibraryEventType.NEW)
                .book(book)
                .build();
        consumerRecord = new ConsumerRecord<>(kafkaTopic, 3, 0, libraryEvent.getLibraryEventId(), objectMapper.writeValueAsString(libraryEvent));
        when(objectMapperMock.readValue(consumerRecord.value(), LibraryEvent.class)).thenReturn(libraryEvent);
        assertThrows(IllegalArgumentException.class, () -> testClass.processLibraryEvent(consumerRecord), () -> "BookId should be null for new library events");
    }

    @Test
    void processUpdateLibraryEvent_bookIdNull() throws JsonProcessingException {
        book = Book.builder()
                .bookId(null)
                .name("aName")
                .author("anAuthor")
                .build();
        libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .libraryEventType(LibraryEventType.UPDATE)
                .book(book)
                .build();
        consumerRecord = new ConsumerRecord<>(kafkaTopic, 3, 0, libraryEvent.getLibraryEventId(), objectMapper.writeValueAsString(libraryEvent));
        when(objectMapperMock.readValue(consumerRecord.value(), LibraryEvent.class)).thenReturn(libraryEvent);
        assertThrows(IllegalArgumentException.class, () -> testClass.processLibraryEvent(consumerRecord), () -> "BookId should not be null for update library events");
    }

    @Test
    void processUpdateLibraryEvent_emptyBook() throws JsonProcessingException {
        book = Book.builder()
                .bookId(123)
                .name("aName")
                .author("anAuthor")
                .build();
        libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .libraryEventType(LibraryEventType.UPDATE)
                .book(book)
                .build();
        consumerRecord = new ConsumerRecord<>(kafkaTopic, 3, 0, libraryEvent.getLibraryEventId(), objectMapper.writeValueAsString(libraryEvent));
        when(objectMapperMock.readValue(consumerRecord.value(), LibraryEvent.class)).thenReturn(libraryEvent);
        when(booKJpaRepo.findById(book.getBookId())).thenReturn(Optional.empty());
        assertThrows(IllegalArgumentException.class, () -> testClass.processLibraryEvent(consumerRecord), () -> "Provided Book Id is missing");
    }

    @Test
    void processUpdateLibraryEvent_notEmptyBook() throws JsonProcessingException {
        book = Book.builder()
                .bookId(123)
                .name("aName")
                .author("anAuthor")
                .build();
        libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .libraryEventType(LibraryEventType.UPDATE)
                .book(book)
                .build();
        consumerRecord = new ConsumerRecord<>(kafkaTopic, 3, 0, libraryEvent.getLibraryEventId(), objectMapper.writeValueAsString(libraryEvent));
        when(objectMapperMock.readValue(consumerRecord.value(), LibraryEvent.class)).thenReturn(libraryEvent);
        when(booKJpaRepo.findById(book.getBookId())).thenReturn(Optional.of(book));
        testClass.processLibraryEvent(consumerRecord);
    }

    @Test
    void processNewLibraryEvent() throws JsonProcessingException {
        book = Book.builder()
                .bookId(null)
                .name("aName")
                .author("anAuthor")
                .build();
        libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .libraryEventType(LibraryEventType.NEW)
                .book(book)
                .build();
        consumerRecord = new ConsumerRecord<>(kafkaTopic, 3, 0, libraryEvent.getLibraryEventId(), objectMapper.writeValueAsString(libraryEvent));
        when(objectMapperMock.readValue(consumerRecord.value(), LibraryEvent.class)).thenReturn(libraryEvent);
        when(libraryEventJpaRepo.save(libraryEvent)).thenReturn(null);
        assertNull(libraryEvent.getBook().getLibraryEvent());
        testClass.processLibraryEvent(consumerRecord);
        assertNotNull(libraryEvent.getBook().getLibraryEvent());
    }

    @Test
    void processUpdateLibraryEvent() throws JsonProcessingException {
        book = Book.builder()
                .bookId(123)
                .name("aName")
                .author("anAuthor")
                .build();
        libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .libraryEventType(LibraryEventType.UPDATE)
                .book(book)
                .build();
        consumerRecord = new ConsumerRecord<>(kafkaTopic, 3, 0, libraryEvent.getLibraryEventId(), objectMapper.writeValueAsString(libraryEvent));
        when(objectMapperMock.readValue(consumerRecord.value(), LibraryEvent.class)).thenReturn(libraryEvent);
        when(booKJpaRepo.findById(book.getBookId())).thenReturn(Optional.of(book));
        when(libraryEventJpaRepo.save(libraryEvent)).thenReturn(null);
        assertNull(libraryEvent.getBook().getLibraryEvent());
        testClass.processLibraryEvent(consumerRecord);
        assertNotNull(libraryEvent.getBook().getLibraryEvent());
    }
}