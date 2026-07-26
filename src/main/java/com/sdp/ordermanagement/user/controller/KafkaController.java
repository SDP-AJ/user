package com.sdp.ordermanagement.user.controller;

import com.sdp.ordermanagement.user.dto.UserEvent;
import com.sdp.ordermanagement.user.service.Listener;
import com.sdp.ordermanagement.user.service.Producer;
import com.sdp.ordermanagement.user.service.DynamicConsumerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/v1/kafka")
public class KafkaController {

    @Autowired
    private Producer producer;

    @Autowired
    private Listener listener;

    @Autowired
    private DynamicConsumerService dynamicConsumerService;

    @PostMapping("/send")
    public void produceMessage(
            @RequestParam String topic,
            @RequestParam(required = false) Integer partition,
            @RequestParam(required = false) String key,
            @RequestBody UserEvent event
    ) {
        producer.sendUserEvent(topic, partition, key, event);
    }

    @GetMapping("/receive")
    public List<UserEvent> getMessage() {
        return listener.getMessages();
    }

    // ✅ New dynamic real-time consumer endpoint
    @GetMapping("/consume")
    public List<UserEvent> consumeMessages(
            @RequestParam String topic,
            @RequestParam(required = false) Integer partition,
            @RequestParam(defaultValue = "dynamic-group") String groupId,
            @RequestParam(required = false) Long fromTimestamp
    ) {
        return dynamicConsumerService.consume(
                topic,
                partition,
                groupId,
                fromTimestamp
        );
    }
}