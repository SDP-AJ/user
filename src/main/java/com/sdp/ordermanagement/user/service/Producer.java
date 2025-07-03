package com.sdp.ordermanagement.user.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class Producer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessageToTopic(String message) {
        // Option 1: Automatic partitioning
        //kafkaTemplate.send("anand-topic", message);

        // Option 2 (optional): With key
       //  kafkaTemplate.send("anand-topic", "user-123", message+" 2");

        // Option 3 (optional): Target specific partition
         kafkaTemplate.send("anand-topic", 0, "user-123", message+" 3");
    }
}
