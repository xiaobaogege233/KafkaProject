package com.zchd.producer;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

@Component
@Slf4j
@Setter
public class TestProducer {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @Value("${spring.kafka.topic}")
    private String topic;

    /**
     * 发送kafka消息
     */
    public void sendTopic(String jsonString) {
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, jsonString);
        future.addCallback(o -> log.info("kafka消息发送成功:{}" , jsonString), throwable -> log.error("kafka消息发送失败:{}" , jsonString));
    }

    public void sendTopicPartition(String jsonString) {
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, 1, null, jsonString);
        future.addCallback(o -> log.info("kafka消息发送成功:{}" , jsonString), throwable -> log.error("kafka消息发送失败:{}" , jsonString));
    }
}
