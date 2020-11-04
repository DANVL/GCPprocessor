package com.processor.gcpprocessor.service.impl;

import com.processor.gcpprocessor.config.Constants;
import com.processor.gcpprocessor.data.entity.Message;
import com.processor.gcpprocessor.service.FileProcessorService;
import com.processor.gcpprocessor.service.MessageService;
import com.processor.gcpprocessor.service.NotificationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.logging.Logger;

@Service
public class MessageServiceImpl implements MessageService {

    private final Logger log = Logger.getLogger(MessageServiceImpl.class.getName());
    private final FileProcessorService fileProcessorService;

    @Autowired
    public MessageServiceImpl(FileProcessorService fileProcessorService) {
        this.fileProcessorService = fileProcessorService;
    }

    @Override
    public void messageProcess(Message message) {
        String file = message.getAttributes().getOrDefault("objectId", null);
        String event = message.getAttributes().getOrDefault("eventType",null);

        if (event.equals("OBJECT_FINALIZE") && file.endsWith(".avro")) {

            log.info("Processing "+file+" file");

            fileProcessorService.runProcessor(Constants.AVRO_SOURCE_URI_PATH + file);

        } else if(event.equals("OBJECT_FINALIZE")){
            log.warning(file+ " extension is not \".avro\". Skipping...");
        }
    }
}
