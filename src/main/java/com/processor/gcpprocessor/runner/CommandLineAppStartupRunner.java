package com.processor.gcpprocessor.runner;

import com.processor.gcpprocessor.service.PubSubListenerService;
import com.processor.gcpprocessor.service.impl.FileProcessorServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.logging.Logger;

@Component
public class CommandLineAppStartupRunner implements CommandLineRunner {


    private final PubSubListenerService pubSubListenerService;

    private final Logger log = Logger.getLogger(FileProcessorServiceImpl.class.getName());

    @Autowired
    public CommandLineAppStartupRunner(PubSubListenerService pubSubListenerService) {
        this.pubSubListenerService = pubSubListenerService;
    }

    @Override
    public void run(String...args) {
        log.info("runs");
        pubSubListenerService.listenToMessages();
    }
}
