package com.processor.gcpprocessor.functions;

import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.processor.gcpprocessor.data.entity.example.gcp.GcsEvent;

import java.util.logging.Logger;

public class MyLogger implements BackgroundFunction<GcsEvent> {
    private static final Logger logger = Logger.getLogger(MyLogger.class.getName());

    @Override
    public void accept(GcsEvent event, Context context) {
        logger.info("Event: " + context.eventId());
        logger.info("Event Type: " + context.eventType());
        logger.info("Bucket: " + event.getBucket());
        logger.info("File: " + event.getName());
        logger.info("Metageneration: " + event.getMetageneration());
        logger.info("Created: " + event.getTimeCreated());
        logger.info("Updated: " + event.getUpdated());
    }

}
