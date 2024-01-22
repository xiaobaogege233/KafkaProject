package com.zchd.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zchd.entity.UserInfo;
import com.zchd.producer.TestProducer;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Setter
@Slf4j
@RestController
public class KafkaController {

    @Autowired
    private TestProducer testProducer;

    @Autowired
    private ObjectMapper objectMapper;

    @RequestMapping("/send_topic")
    public void sendTopic(){
        UserInfo userInfo = new UserInfo();
        userInfo.setName("鲍安和");
        userInfo.setAge(18);
        userInfo.setIdcard("421281199701240015");
        userInfo.setSalary(50);
        try {
            testProducer.sendTopic(objectMapper.writeValueAsString(userInfo));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @RequestMapping("/send_topic_partition")
    public void sendTopicPartition(){
        UserInfo userInfo = new UserInfo();
        userInfo.setName("鲍安和");
        userInfo.setAge(18);
        userInfo.setIdcard("421281199701240015");
        userInfo.setSalary(50);
        try {
            testProducer.sendTopicPartition(objectMapper.writeValueAsString(userInfo));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
