package com.processor.gcpprocessor.service;

public interface FileProcessor {
    String runProcessor(String datasetName, String tableName, String avroSourceUri);
}
