package com.processor.gcpprocessor.service.impl;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.processor.gcpprocessor.service.SchemaService;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class SchemaServiceImpl implements SchemaService {


    @Override
    public Schema nonOptionalSchema(){
        org.apache.avro.Schema schema = null;

        try {
            //Getting AVRO schema
            schema = new org.apache.avro.Schema.Parser()
                    .parse(new File("src/main/resources/avro/Client.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
        }


        //Extracting non-optional fields
        List<Field> bqFields = new ArrayList<>();
        if (schema != null) {
            for(org.apache.avro.Schema.Field field : schema.getFields()) {
                if(field.schema().isNullable()){
                    bqFields.add(Field.of(field.name(), castToBQType(field.schema().getTypes().get(0).getType())));
                }

            }
        }

        return com.google.cloud.bigquery.Schema.of(bqFields);
    }

    //Casting AVRO primitive types to BigQuery types
    private StandardSQLTypeName castToBQType(org.apache.avro.Schema.Type type){
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


}
