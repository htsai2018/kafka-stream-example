package com.hct.kafka.messaging;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Arrays.asList;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles("test")
@DirtiesContext
public class KafkaMessagingApplicationTests {

    @ClassRule
    public static EmbeddedKafkaRule broker = new EmbeddedKafkaRule(1);

    @Autowired
    private KafkaProperties kafkaProperties;

    @Autowired
    private KafkaAdmin kafkaAdmin;

    @BeforeClass
    public static void setup() {
        System.setProperty("spring.kafka.bootstrap-servers",
                broker.getEmbeddedKafka().getBrokersAsString());
    }

    @Test
    public void contextLoads() {
        String bootstrapServer = broker.getEmbeddedKafka().getBrokersAsString();
        assertThat(kafkaProperties.getBootstrapServers()).containsExactly(bootstrapServer);
        List<String> bootstrapServers = (List<String>) kafkaAdmin.getConfig().get("bootstrap.servers");
        assertThat(bootstrapServers).containsExactly(bootstrapServer);
    }

}

