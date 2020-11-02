package com.processor.gcpprocessor.runner;

import com.processor.gcpprocessor.service.PubSubListenerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class CommandLineAppStartupRunner implements CommandLineRunner {


    private final PubSubListenerService pubSubListenerService;

    @Autowired
    public CommandLineAppStartupRunner(PubSubListenerService pubSubListenerService) {
        this.pubSubListenerService = pubSubListenerService;
    }

    @Override
    public void run(String...args) {
        pubSubListenerService.listenToMessages();
    }
}
