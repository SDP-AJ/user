package com.sdp.ordermanagement.user.controller;

import com.sdp.ordermanagement.user.service.Listener;
import com.sdp.ordermanagement.user.service.Producer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/v1/kafka")
public class KafkaController {

    @Autowired
    Producer producer;

    @Autowired
    Listener listener;

    @GetMapping("/send")
    public void produceMessage(@RequestParam String message) {
        producer.sendMessageToTopic(message);
    }

    @GetMapping("/receive")
    public List<String> getMessage() {
        return listener.getMessages();
    }
}
