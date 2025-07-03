package com.sdp.ordermanagement.user.service;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
public class Listener implements ConsumerSeekAware {

    private final List<String> messages = new CopyOnWriteArrayList<>();
    private long seekTime;

    @KafkaListener(topics = "anand-topic", groupId = "my-group")
    public void listen(ConsumerRecord<String, String> record) {
        if (record.timestamp() >= seekTime) {
            System.out.println("🟢 Received: " + record.value());
            messages.add(record.value());
        } else {
            System.out.println("⏳ Skipping old message: " + record.value() + " with timestamp " + record.timestamp());
        }
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        // Compute seek time *now*, not during class init
        seekTime = System.currentTimeMillis() - Duration.ofSeconds(10).toMillis();

        for (TopicPartition partition : assignments.keySet()) {
            callback.seekToTimestamp(partition.topic(), partition.partition(), seekTime);
            System.out.println("⏱ Seeking to: " + seekTime + " for partition " + partition.partition());
        }
    }

    public List<String> getMessages() {
        return messages;
    }
}
