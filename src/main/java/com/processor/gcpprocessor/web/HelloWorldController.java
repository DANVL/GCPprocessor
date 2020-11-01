package com.processor.gcpprocessor.web;

import com.processor.gcpprocessor.service.FileProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

@RestController
public class HelloWorldController {

    private final FileProcessor fileProcessor;

    private final String datasetName = "avrotables";
    private final String tableName = "table1";
    private final String avroSourceUri = "gs://apt-index-293821_cloudbuild/client.avro";

    @Autowired
    public HelloWorldController(FileProcessor fileProcessor) {
        this.fileProcessor = fileProcessor;
    }

    @GetMapping("/")
    String hello() throws IOException {
        return fileProcessor.runProcessor(datasetName, tableName, avroSourceUri);
    }
}
