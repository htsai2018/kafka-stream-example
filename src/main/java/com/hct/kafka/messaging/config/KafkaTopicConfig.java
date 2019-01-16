package com.hct.kafka.messaging.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
@EnableKafka
@Slf4j
public class KafkaTopicConfig {

    @KafkaListener(topics = "${spring.kafka.template.default-topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(ConsumerRecord<?, ?> record) {
        log.info("consume topic:{}, partition: {}, offset: {} \nkey: {}\nvalue: {}", record.topic() , record.partition(), record.offset(), record.key(), record.value());
    }
}
