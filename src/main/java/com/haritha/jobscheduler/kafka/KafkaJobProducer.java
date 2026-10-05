package com.haritha.jobscheduler.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaJobProducer {

    private static final String TOPIC = "job-execution";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaJobProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendJob(Long jobId) {

        String key = "job-" + jobId;
        String message = String.valueOf(jobId);

        kafkaTemplate.send(TOPIC, key, message);
    }
}