package com.processor.gcpprocessor.runner;

import com.processor.gcpprocessor.service.PubSubListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class CommandLineAppStartupRunner implements CommandLineRunner {


    private final PubSubListener pubSubListener;

    @Autowired
    public CommandLineAppStartupRunner(PubSubListener pubSubListener) {
        this.pubSubListener = pubSubListener;
    }

    @Override
    public void run(String...args) {
        pubSubListener.listenToMessages();
    }
}
