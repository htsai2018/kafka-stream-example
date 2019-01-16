package com.hct.kafka.messaging.service;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles("test")
@DirtiesContext
public class MessageServiceTest {

    @ClassRule
    public static EmbeddedKafkaRule broker = new EmbeddedKafkaRule(1, false, "my-topic");

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Autowired
    private KafkaProperties kafkaProperties;

    @Autowired
    private MessageService messageService;

    @BeforeClass
    public static void init() {
        System.setProperty("spring.kafka.bootstrap-servers",
                broker.getEmbeddedKafka().getBrokersAsString());
    }

    @Test
    public void sendMessage_givenDefaultTopicAndAStringForMessageBody_sendsTheMessage() throws Exception {
        Map<String, Object> config = kafkaProperties.buildConsumerProperties();
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group2");
        ConsumerFactory<?, String> cf = new DefaultKafkaConsumerFactory<>(config);
        Consumer<?, String> consumer = cf.createConsumer();
        broker.getEmbeddedKafka().consumeFromAllEmbeddedTopics(consumer);

        ProducerRecord producerRecord = new ProducerRecord("my-topic", "my-key", "my test message");
        messageService.sendMessage(producerRecord);

        ConsumerRecord<?, String> record = KafkaTestUtils.getSingleRecord(consumer, "my-topic");
        assertThat(record).isNotNull();
        assertThat(record.key()).isEqualTo("my-key");
        assertThat(record.value()).isEqualTo("my test message");
    }
}