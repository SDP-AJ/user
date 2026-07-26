package com.sdp.ordermanagement.user.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Service
public class KafkaAdminService {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public void createTopic(String topicName, int numPartitions, short replicationFactor) {

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(props)) {

            if (topicExists(adminClient, topicName)) {
                return;
            }

            NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
            CreateTopicsResult result =
                    adminClient.createTopics(Collections.singleton(newTopic));

            result.all().get(); // wait for completion

        } catch (Exception e) {
            throw new RuntimeException("Failed to create topic: " + topicName, e);
        }
    }

    private boolean topicExists(AdminClient adminClient, String topicName)
            throws ExecutionException, InterruptedException {

        DescribeTopicsResult result =
                adminClient.describeTopics(Collections.singletonList(topicName));

        KafkaFuture<TopicDescription> future =
                result.values().get(topicName);

        try {
            future.get();
            return true;
        } catch (ExecutionException e) {
            return false;
        }
    }
}