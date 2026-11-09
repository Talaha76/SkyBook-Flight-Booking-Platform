package com.skybook.inventory.config;

import java.util.List;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class KafkaConfig {

    /** Creates the topics if they don't exist yet (the Node services do the same with ensureTopics). */
    @Bean
    public KafkaAdmin.NewTopics skybookTopics(AppProperties props) {
        List<NewTopic> topics = List.of(props.topics().booking(), props.topics().inventory(), props.topics().payment()).stream()
                .map(name -> TopicBuilder.name(name).partitions(props.kafkaPartitions()).replicas(1).build())
                .toList();
        return new KafkaAdmin.NewTopics(topics.toArray(NewTopic[]::new));
    }

    /**
     * Listener container: one consumer thread per partition (concurrency), MANUAL_IMMEDIATE acks that the
     * listener sends only after the DB transaction commits, and an error handler that retries a failing
     * record with exponential backoff forever. A record is never skipped because the database is down;
     * malformed messages are handled (skipped) by the listener itself, so they can't block a partition.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory,
            AppProperties props) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        factory.setConcurrency(props.kafkaConcurrency());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        ExponentialBackOff backOff = new ExponentialBackOff(500L, 2.0);
        backOff.setMaxInterval(30_000L);
        backOff.setMaxElapsedTime(Long.MAX_VALUE);
        factory.setCommonErrorHandler(new DefaultErrorHandler(backOff));
        return factory;
    }
}
