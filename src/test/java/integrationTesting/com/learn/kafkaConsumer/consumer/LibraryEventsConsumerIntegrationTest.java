package com.learn.kafkaConsumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafkaConsumer.entity.Book;
import com.learn.kafkaConsumer.entity.LibraryEvent;
import com.learn.kafkaConsumer.entity.LibraryEventType;
import com.learn.kafkaConsumer.repository.jpa.BooKJpaRepo;
import com.learn.kafkaConsumer.repository.jpa.LibraryEventJpaRepo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
@EmbeddedKafka(topics = {"${spring.kafka.template.default-topic}", "${spring.local.kafka.topic}"}, partitions = 3)
@TestPropertySource(properties =
        {
                "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
                "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"
        })
class LibraryEventsConsumerIntegrationTest {

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    LibraryEventJpaRepo libraryEventJpaRepo;

    @Autowired
    BooKJpaRepo booKJpaRepo;

    ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry.getListenerContainers()) {
            assertEquals(Arrays.toString(Objects.requireNonNull(messageListenerContainer.getContainerProperties().getTopics())), Arrays.toString(embeddedKafkaBroker.getTopics().toArray()));
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic() * 2);
        }
    }

    @AfterEach
    void tearDown() {
        libraryEventJpaRepo.deleteAll();
    }

    @Test
    @Timeout(60)
    public void publishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        Thread.sleep(10000);
        Book book = Book.builder()
                .bookId(null)
                .author("anAuthor")
                .name("aName")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .libraryEventType(LibraryEventType.NEW)
                .book(book)
                .build();
        String libraryEventValue = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEventValue).get();
        Thread.sleep(10000);
        List<LibraryEvent> libraryEventList = (List<LibraryEvent>) libraryEventJpaRepo.findAll();
        assert libraryEventList.size() == 1;
        libraryEventJpaRepo.findAll().forEach(
                event -> {
                    assertNotNull(event);
                    assertNotNull(event.getBook());
                    assertNotNull(event.getLibraryEventId());
                    assertNotNull(event.getBook().getBookId());
                }
        );
        List<Book> bookList = (List<Book>) booKJpaRepo.findAll();
        assert bookList.size() == 1;
        booKJpaRepo.findAll().forEach(
                b -> {
                    assertNotNull(b);
                    assertNotNull(b.getBookId());
                }
        );
    }

    @Test
    @Timeout(60)
    public void publishUpdateLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        Thread.sleep(10000);
        Book expectedBook = Book.builder()
                .bookId(null)
                .author("anAuthor")
                .name("aName")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .libraryEventType(LibraryEventType.UPDATE)
                .book(expectedBook)
                .build();

        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventJpaRepo.save(libraryEvent);
        Thread.sleep(3000);
        List<LibraryEvent> libraryEvents = (List<LibraryEvent>) libraryEventJpaRepo.findAll();
        assert libraryEvents.size() == 1;
        libraryEvent = libraryEvents.get(0);
        List<Book> bookList = (List<Book>) booKJpaRepo.findAll();
        assert bookList.size() == 1;
        Book actualBook = bookList.get(0);

        actualBook = Book.builder()
                .bookId(actualBook.getBookId())
                .author("author is updated")
                .name("name also updated")
                .build();
        libraryEvent = LibraryEvent.builder()
                .libraryEventId(libraryEvent.getLibraryEventId())
                .libraryEventType(LibraryEventType.UPDATE)
                .book(actualBook)
                .build();
        String libraryEventValue = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEventValue).get();
        Thread.sleep(10000);
        List<LibraryEvent> libraryEventList = (List<LibraryEvent>) libraryEventJpaRepo.findAll();
        assert libraryEventList.size() == 1;
        libraryEventJpaRepo.findAll().forEach(
                event -> {
                    assertNotNull(event);
                    assertNotNull(event.getBook());
                    assertNotNull(event.getLibraryEventId());
                    assertNotNull(event.getBook().getBookId());
                }
        );
        List<Book> books = (List<Book>) booKJpaRepo.findAll();
        assert books.size() == 1;
        booKJpaRepo.findAll().forEach(
                b -> {
                    assertNotNull(b);
                    assertNotNull(b.getBookId());
                }
        );

        actualBook = books.get(0);
        assertEquals(expectedBook.getBookId(), actualBook.getBookId());
        assertEquals("name also updated", actualBook.getName());
        assertEquals("author is updated", actualBook.getAuthor());
    }


}