package com.zchd.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zchd.entity.UserInfo;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@Setter
public class TestConsumer {

    @Autowired
    private ObjectMapper objectMapper;

//    @KafkaListener(topics = "test")
//    public void consume(String message){
//        try {
//            log.info("topic partition process message");
//            log.info(message);
//            UserInfo userInfo = objectMapper.readValue(message, UserInfo.class);
//            System.out.println(userInfo);
//        } catch (JsonProcessingException e) {
//            throw new RuntimeException(e);
//        }
//    }

    @KafkaListener(topicPartitions = @TopicPartition(topic = "test", partitions = {"0"}))
    public void consume_partition(String message){
        try {
            log.info("partition0 process message");
            log.info(message);
            UserInfo userInfo = objectMapper.readValue(message, UserInfo.class);
            System.out.println(userInfo);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @KafkaListener(topicPartitions = @TopicPartition(topic = "test", partitions = {"1"}))
    public void consume_partition1(String message){
        try {
            log.info("partition1 process message");
            log.info(message);
            UserInfo userInfo = objectMapper.readValue(message, UserInfo.class);
            System.out.println(userInfo);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
