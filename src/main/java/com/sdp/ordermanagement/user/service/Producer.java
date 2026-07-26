package com.sdp.ordermanagement.user.service;

import com.sdp.ordermanagement.user.dto.UserEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class Producer {

    @Autowired
    private KafkaTemplate<String, UserEvent> kafkaTemplate;

    public void sendUserEvent(
            String topic,
            Integer partition,
            String key,
            UserEvent event
    ) {

        if (partition != null) {
            kafkaTemplate.send(topic, partition, key, event);
        } else if (key != null) {
            kafkaTemplate.send(topic, key, event);
        } else {
            kafkaTemplate.send(topic, event);
        }
    }
}