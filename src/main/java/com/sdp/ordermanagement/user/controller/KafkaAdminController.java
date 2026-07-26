package com.sdp.ordermanagement.user.controller;

import com.sdp.ordermanagement.user.service.KafkaAdminService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/admin/kafka")
public class KafkaAdminController {

    @Autowired
    private KafkaAdminService kafkaAdminService;

    @PostMapping("/create-topic")
    public String createTopic(
            @RequestParam String topicName,
            @RequestParam int partitions,
            @RequestParam short replicationFactor
    ) {

        kafkaAdminService.createTopic(topicName, partitions, replicationFactor);
        return "Topic creation requested for: " + topicName;
    }
}
