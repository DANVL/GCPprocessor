package com.processor.gcpprocessor.service.impl;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.processor.gcpprocessor.config.Constants;
import com.processor.gcpprocessor.service.FileProcessorService;
import com.processor.gcpprocessor.service.PubSubListenerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.logging.Logger;

@Service
public class PubSubListenerServiceImpl implements PubSubListenerService {

    private final Logger log = Logger.getLogger(PubSubListenerServiceImpl.class.getName());
    private final FileProcessorService fileProcessorService;

    @Autowired
    public PubSubListenerServiceImpl(FileProcessorService fileProcessorService) {
        this.fileProcessorService = fileProcessorService;
    }

    // Instantiate an asynchronous message receiver
    private final MessageReceiver receiver =
            (PubsubMessage message, AckReplyConsumer consumer) -> {
                listenerOnReceive(message);
                consumer.ack();
            };


    @Override
    public void listenToMessages() {
        ProjectSubscriptionName subscriptionName =
                ProjectSubscriptionName.of(Constants.PROJECT_ID, Constants.TOPIC_ID);

        Subscriber subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();

        // Start the subscriber
        subscriber.startAsync().awaitRunning();
        log.info("Listening for messages on "+ subscriptionName.toString());
    }

    // Actions on received message
    private void listenerOnReceive(PubsubMessage message) {
        String file = message.getAttributesOrDefault("objectId", "null");
        String event = message.getAttributesOrDefault("eventType","null");

        if (event.equals("OBJECT_FINALIZE") && file.endsWith(".avro")) {
            log.info("Processing "+file+" file");
            fileProcessorService.runProcessor(Constants.AVRO_SOURCE_URI_PATH + file);

        } else if(event.equals("OBJECT_FINALIZE")){
            log.warning(file+ " extension is not .avro. SKIP");
        }
    }
}
