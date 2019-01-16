package com.hct.kafka.messaging.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;

@Service
@EnableKafka
@Slf4j
public class TopicService {

    private KafkaAdmin kafkaAdmin;

    public TopicService(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    public void createTopic(NewTopic newTopic) throws Exception {
        AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfig());
        try {
                CreateTopicsResult topicResults = adminClient.createTopics(asList(newTopic));
                    topicResults.all().get(10, TimeUnit.SECONDS);

        }
        finally {
            adminClient.close(10, TimeUnit.SECONDS);
        }
    }

    public boolean existTopic(String topicName) throws Exception {
        AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfig());
        try {
            DescribeTopicsResult topics = adminClient.describeTopics(Arrays.asList(topicName));
            Map<String, TopicDescription> results = topics.all().get();
            TopicDescription topicDescription = results.get(topicName);
            return topicDescription != null && !topicDescription.partitions().isEmpty();
        }
        catch (UnknownTopicOrPartitionException unknownTopicOrPartitionException) {
            return false;
        }
        catch (Throwable err) {
            if (err.getCause() instanceof UnknownTopicOrPartitionException) {
                return false;
            }
            throw err;
        }
        finally {
            adminClient.close(10, TimeUnit.SECONDS);
        }
    }

    public TopicDescription getTopicDescription(String topicName) throws Exception {
        AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfig());
        try {
            DescribeTopicsResult topics = adminClient.describeTopics(Arrays.asList(topicName));
            Map<String, TopicDescription> results = topics.all().get();
            return results.get(topicName);
        }
        catch (UnknownTopicOrPartitionException unknownTopicOrPartitionException) {
            return null;
        }
        catch (Throwable err) {
            if (err.getCause() instanceof UnknownTopicOrPartitionException) {
                return null;
            }
            throw err;
        }
        finally {
            adminClient.close(10, TimeUnit.SECONDS);
        }
    }

    public Set<String> listAllTopics() throws Exception {
        AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfig());
        try {
            ListTopicsResult listTopicsResult = adminClient.listTopics();
            return listTopicsResult.names().get();
        }
        finally {
            adminClient.close(10, TimeUnit.SECONDS);
        }
    }

}
