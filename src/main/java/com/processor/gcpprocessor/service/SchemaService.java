package com.processor.gcpprocessor.service;

import com.google.cloud.bigquery.Schema;

public interface SchemaService {
    Schema nonOptionalSchema();
}
