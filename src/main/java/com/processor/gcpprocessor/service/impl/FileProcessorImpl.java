package com.processor.gcpprocessor.service.impl;

import org.apache.avro.Schema;
import com.google.cloud.bigquery.*;
import com.processor.gcpprocessor.service.FileProcessor;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
                    LoadJobConfiguration.newBuilder(tableId2, avroSourceUri)
                            .setFormatOptions(FormatOptions.avro())
                            // Set the write disposition to overwrite existing table data
                            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
                            .setSchema(nonOptionalSchema())
                            .build();

            System.out.println(avroSourceUri);


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




    private StandardSQLTypeName castToBQType(Schema.Type type){
        switch (type){
            case INT:
            case LONG:
                return StandardSQLTypeName.INT64;
            case FLOAT:
            case DOUBLE:
                return StandardSQLTypeName.FLOAT64;
            case BOOLEAN:
                return StandardSQLTypeName.BOOL;
            case BYTES:
                return StandardSQLTypeName.BYTES;
            case STRING:
            default:
                return StandardSQLTypeName.STRING;

        }
    }

    private com.google.cloud.bigquery.Schema nonOptionalSchema(){
        Schema schema = null;

        try {
            schema = new Schema.Parser().parse(new File("src/main/resources/avro/Client.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
        }


       // Schema newSchema = Schema.createRecord(schema.getName(),schema.getDoc(),schema.getNamespace(),schema.isError());

        List<Field> bqFields = new ArrayList<>();
        for(Schema.Field field : schema.getFields()) {
            if(field.schema().isNullable()){
                bqFields.add(Field.of(field.name(), castToBQType(field.schema().getTypes().get(0).getType())));
            }

        }

        return com.google.cloud.bigquery.Schema.of(bqFields);
    }

}

