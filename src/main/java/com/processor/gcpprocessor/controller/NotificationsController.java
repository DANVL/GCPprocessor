package com.processor.gcpprocessor.controller;

import com.processor.gcpprocessor.data.entity.Message;
import com.processor.gcpprocessor.service.MessageService;
import com.processor.gcpprocessor.data.entity.Body;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class NotificationsController {


    private final MessageService messageService;

    @Autowired
    public NotificationsController(MessageService messageService) {
        this.messageService = messageService;
    }

    @PostMapping
    public ResponseEntity<?> receiveMessage(@RequestBody Body body) {

        // Get PubSub message from request body.
        Message message = body.getMessage();
        if (message == null) {
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }

        messageService.messageProcess(message);


        return new ResponseEntity<>(HttpStatus.OK);
    }
}
