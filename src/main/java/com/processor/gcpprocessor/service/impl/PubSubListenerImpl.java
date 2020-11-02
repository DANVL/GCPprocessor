package com.processor.gcpprocessor.service.impl;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.processor.gcpprocessor.config.Constants;
import com.processor.gcpprocessor.service.FileProcessor;
import com.processor.gcpprocessor.service.PubSubListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.logging.Logger;

@Service
public class PubSubListenerImpl implements PubSubListener {

    private final Logger log = Logger.getLogger(PubSubListenerImpl.class.getName());
    private final FileProcessor fileProcessor;

    @Autowired
    public PubSubListenerImpl(FileProcessor fileProcessor) {
        this.fileProcessor = fileProcessor;
    }

    // Instantiate an asynchronous message receiver.
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

        // Start the subscriber.
        subscriber.startAsync().awaitRunning();
        System.out.printf("Listening for messages on %s:\n", subscriptionName.toString());
    }

    private void listenerOnReceive(PubsubMessage message) {
        String file = message.getAttributesOrDefault("objectId", "null");
        String event = message.getAttributesOrDefault("eventType","null");
        System.out.println(event);

        if (event.equals("OBJECT_FINALIZE") && file.endsWith(".avro")) {
            log.info("Processing "+file+" file");
            fileProcessor.runProcessor(Constants.DATA_SET, Constants.TABLE_NAME1, Constants.TABLE_NAME2,
                    Constants.AVRO_SOURCE_URI_PATH + file);

        } else if(event.equals("OBJECT_FINALIZE")){
            log.warning(file+ " end is not .avro. SKIP");
        }
    }
}
