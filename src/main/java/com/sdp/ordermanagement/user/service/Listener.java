package com.sdp.ordermanagement.user.service;

import com.sdp.ordermanagement.user.dto.UserEvent;
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

    private final List<UserEvent> messages = new CopyOnWriteArrayList<>();
    private long seekTime;

    @KafkaListener(
            topics = "anand-topic",
            groupId = "my-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(ConsumerRecord<String, UserEvent> record) {

        if (record.timestamp() >= seekTime) {
            System.out.println("🟢 Received: " + record.value());
            messages.add(record.value());
        } else {
            System.out.println("⏳ Skipping old message: " + record.value());
        }
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments,
                                     ConsumerSeekCallback callback) {

        seekTime = System.currentTimeMillis()
                - Duration.ofSeconds(10).toMillis();

        for (TopicPartition partition : assignments.keySet()) {
            callback.seekToTimestamp(
                    partition.topic(),
                    partition.partition(),
                    seekTime
            );
        }
    }

    public List<UserEvent> getMessages() {
        return messages;
    }
}