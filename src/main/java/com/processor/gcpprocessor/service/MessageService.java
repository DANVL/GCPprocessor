package com.processor.gcpprocessor.service;

import com.google.pubsub.v1.PubsubMessage;
import com.processor.gcpprocessor.data.entity.Message;

public interface MessageService {
    void messageProcess(Message message);
}
