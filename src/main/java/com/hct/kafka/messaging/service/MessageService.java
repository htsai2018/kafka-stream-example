package com.hct.kafka.messaging.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class MessageService {

    private KafkaTemplate kafkaTemplate;

    public MessageService(KafkaTemplate kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(ProducerRecord producerRecord) {
        kafkaTemplate.send(producerRecord);
    }

}
