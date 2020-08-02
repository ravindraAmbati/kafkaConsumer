package com.learn.kafkaConsumer.repository.jpa;

import com.learn.kafkaConsumer.entity.LibraryEvent;
import org.springframework.data.repository.CrudRepository;

public interface LibraryEventJpaRepo extends CrudRepository<LibraryEvent, Integer> {
}
