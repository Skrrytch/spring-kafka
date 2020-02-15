package com.codenotfound.kafka.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.codenotfound.kafka.kafka.EdigasInTopic;
import com.codenotfound.kafka.producer.Sender;

@RestController
public class MessageRestController {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageRestController.class);

    @Autowired
    private Sender sender;

    @Autowired
    private EdigasInTopic topic;

    @PostMapping("/send/{id}")
    public String sendMessage(@RequestBody String message, @PathVariable String id) {
        LOGGER.info("Sending message {} with id {}...", message, id);
        sender.send(topic, message, id);
        return "done";
    }
}
