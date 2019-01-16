package com.hct.kafka.messaging.service;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles("test")
@DirtiesContext
public class TopicServiceTest {

    @ClassRule
    public static EmbeddedKafkaRule broker = new EmbeddedKafkaRule(1, false, "myTopic-1");

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Autowired
    private TopicService topicService;

    @BeforeClass
    public static void init() {
        System.setProperty("spring.kafka.bootstrap-servers",
                broker.getEmbeddedKafka().getBrokersAsString());
    }

    @Test
    public void existTopic_givenAnExistingTopicName_returnsTrue() throws Exception {
        boolean exist = topicService.existTopic("myTopic-1");
        assertThat(exist).isTrue();
    }

    @Test
    public void existTopic_givenANotExistingTopicName_returnsFalse() throws Exception {
        boolean exist = topicService.existTopic("myTopic-2");
        assertThat(exist).isFalse();
    }

    @Test
    public void createTopic_givenAnExistingTopic_throwsException() throws Exception {
        expectedException.expect(ExecutionException.class);
        expectedException.expectMessage("org.apache.kafka.common.errors.TopicExistsException: Topic 'myTopic-1' already exists.");

        NewTopic newTopic = new NewTopic("myTopic-1", 2, (short) 1);
        topicService.createTopic(newTopic);
    }

    @Test
    public void createTopic_givenANotExistingTopic_createsTheTopic() throws Exception {
        NewTopic newTopic = new NewTopic(UUID.randomUUID().toString(), 1, (short) 1);
        topicService.createTopic(newTopic);

        boolean exist = topicService.existTopic(newTopic.name());
        assertThat(exist).isTrue();
    }

    @Test
    public void getTopicDescription_givenAnExistingTopicName_returnsTopicDescription() throws Exception {
        TopicDescription topicDescription = topicService.getTopicDescription("myTopic-1");
        assertThat(topicDescription).isNotNull();
        assertThat(topicDescription.name()).isEqualTo("myTopic-1");
        assertThat(topicDescription.isInternal()).isFalse();
        assertThat(topicDescription.partitions()).hasSize(2);
    }


    @Test
    public void getTopicDescription_givenANotExistingTopicName_returnsNull() throws Exception {
        TopicDescription topicDescription = topicService.getTopicDescription("test");
        assertThat(topicDescription).isNull();
    }

    @Test
    public void listAllTopics() throws Exception {
        Set<String> allTopics = topicService.listAllTopics();
        assertThat(allTopics).contains("myTopic-1");
    }
}