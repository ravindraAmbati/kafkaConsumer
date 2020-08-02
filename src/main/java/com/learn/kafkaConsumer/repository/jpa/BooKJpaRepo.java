package com.learn.kafkaConsumer.repository.jpa;

import com.learn.kafkaConsumer.entity.Book;
import org.springframework.data.repository.CrudRepository;

public interface BooKJpaRepo extends CrudRepository<Book, Integer> {
}
