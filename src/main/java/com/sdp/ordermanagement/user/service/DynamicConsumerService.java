package com.sdp.ordermanagement.user.service;

import com.sdp.ordermanagement.user.dto.UserEvent;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;

@Service
public class DynamicConsumerService {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public List<UserEvent> consume(
            String topic,
            Integer partition,
            String groupId,
            Long fromTimestamp
    ) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        List<UserEvent> results = new ArrayList<>();

        try (KafkaConsumer<String, UserEvent> consumer =
                     new KafkaConsumer<>(
                             props,
                             new StringDeserializer(),
                             new JsonDeserializer<>(UserEvent.class))) {

            if (partition != null) {

                TopicPartition tp = new TopicPartition(topic, partition);
                consumer.assign(Collections.singletonList(tp));

                if (fromTimestamp != null) {
                    Map<TopicPartition, Long> timestamps =
                            Collections.singletonMap(tp, fromTimestamp);

                    Map<TopicPartition, OffsetAndTimestamp> offsets =
                            consumer.offsetsForTimes(timestamps);

                    OffsetAndTimestamp oat = offsets.get(tp);
                    if (oat != null) {
                        consumer.seek(tp, oat.offset());
                    }
                }

            } else {
                consumer.subscribe(Collections.singletonList(topic));
            }

            ConsumerRecords<String, UserEvent> records =
                    consumer.poll(Duration.ofSeconds(5));

            for (ConsumerRecord<String, UserEvent> record : records) {
                results.add(record.value());
            }

        }

        return results;
    }
}