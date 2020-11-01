package com.processor.gcpprocessor.service.impl;

import com.google.cloud.bigquery.*;
import com.processor.gcpprocessor.service.FileProcessor;
import org.springframework.stereotype.Service;

@Service
public class FileProcessorImpl implements FileProcessor {


    // Initialize client that will be used to send requests. This client only needs to be created
    // once, and can be reused for multiple requests.
    private final BigQuery bigquery;

    public FileProcessorImpl() {
        bigquery = BigQueryOptions.getDefaultInstance().getService();
    }


    @Override
    public String runProcessor(String datasetName,
                               String firstTableName, String secondTableName,
                               String avroSourceUri) {
        try {
            TableId tableId1 = TableId.of(datasetName, firstTableName);
            TableId tableId2 = TableId.of(datasetName, secondTableName);


            LoadJobConfiguration loadConfig =
                    LoadJobConfiguration.newBuilder(tableId1, avroSourceUri)
                            .setFormatOptions(FormatOptions.avro())
                            // Set the write disposition to overwrite existing table data
                            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)

                            .build();

            // Load data from a GCS Avro file into the table
            Job job = bigquery.create(JobInfo.of(loadConfig));

            // Blocks until this load table job completes its execution, either failing or succeeding.
            job = job.waitFor();
            if (job.isDone()) {
                System.out.println("Table is successfully appended by AVRO file loaded from GCS");
            } else {
                System.out.println(
                        "BigQuery was unable to load into the table due to an error:"
                                + job.getStatus().getError());
            }

            return avroSourceUri + " successfully processed";
        } catch (BigQueryException | InterruptedException e) {
            return ("Column not added during load append \n" + e.toString());
        }
    }
}
