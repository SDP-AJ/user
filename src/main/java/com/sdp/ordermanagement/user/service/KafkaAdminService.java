package com.sdp.ordermanagement.user.service;



import jakarta.annotation.PostConstruct;
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

    @PostConstruct
    public void createTopicOnStartup() {
        createTopic("anand-topic", 3, (short) 1);
    }

    public void createTopic(String topicName, int numPartitions, short replicationFactor) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(props)) {
            // Check if topic already exists
            if (topicExists(adminClient, topicName)) {
                System.out.println("✅ Topic already exists: " + topicName);
                return;
            }

            // Define new topic
            NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
            CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));
            result.all().get(); // Wait for completion

            System.out.println("🎉 Topic created: " + topicName + " with " + numPartitions + " partitions");
        } catch (Exception e) {
            System.err.println("❌ Failed to create topic: " + e.getMessage());
        }
    }

    private boolean topicExists(AdminClient adminClient, String topicName) throws ExecutionException, InterruptedException {
        DescribeTopicsResult result = adminClient.describeTopics(Collections.singletonList(topicName));
        KafkaFuture<TopicDescription> future = result.values().get(topicName);
        try {
            future.get();
            return true;
        } catch (ExecutionException e) {
            return false;
        }
    }
}

