package com.processor.gcpprocessor.service.impl;

import com.google.cloud.bigquery.*;
import com.processor.gcpprocessor.config.Constants;
import com.processor.gcpprocessor.service.FileProcessorService;
import com.processor.gcpprocessor.service.SchemaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.logging.Logger;

@Service
public class FileProcessorServiceImpl implements FileProcessorService {

    private final Logger log = Logger.getLogger(PubSubListenerServiceImpl.class.getName());

    // Initialize client that will be used to send requests. This client only needs to be created
    // once, and can be reused for multiple requests.
    private final BigQuery bigquery;

    private final SchemaService schemaService;

    @Autowired
    public FileProcessorServiceImpl(SchemaService schemaService) {
        bigquery = BigQueryOptions.getDefaultInstance().getService();
        this.schemaService = schemaService;
    }


    @Override
    public void runProcessor(String path) {
        try {
            TableId tableId1 = TableId.of(Constants.DATA_SET, Constants.TABLE_NAME1);
            TableId tableId2 = TableId.of(Constants.DATA_SET, Constants.TABLE_NAME2);

            LoadJobConfiguration loadConfig1 =
                    LoadJobConfiguration.newBuilder(tableId1, path)
                            .setFormatOptions(FormatOptions.avro())
                            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
                            .build();

            LoadJobConfiguration loadConfig2 =
                    LoadJobConfiguration.newBuilder(tableId2, path)
                            .setFormatOptions(FormatOptions.avro())
                            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
                            //The thing that defines columns in second table
                            .setSchema(schemaService.nonOptionalSchema())
                            .build();


            log.info(jobProcess(loadConfig1, tableId1.getTable()));
            log.info(jobProcess(loadConfig2, tableId2.getTable()));

            log.info("Both tables processed");

        } catch (BigQueryException | InterruptedException e) {
            log.warning("Column not added during load append \n" + e.toString());
        }
    }

    private String jobProcess(LoadJobConfiguration loadConfig, String tableName) throws InterruptedException {

        // Load data from a GCS Avro file into the table
        Job job = bigquery.create(JobInfo.of(loadConfig));

        // Blocks until this load table job completes its execution, either failing or succeeding.
        job = job.waitFor();
        if (job.isDone()) {
            return "Table: " + tableName + " is successfully appended by AVRO file loaded from GCS";
        } else {
            return "BigQuery was unable to load into the table due to an error:"
                    + job.getStatus().getError();
        }
    }
}

