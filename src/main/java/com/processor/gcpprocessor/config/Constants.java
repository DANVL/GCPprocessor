package com.processor.gcpprocessor.config;

public final class Constants {

    // GCP project id
    public static final String PROJECT_ID = "apt-index-293821";

    //
    public static final String DATA_SET = "avro_processed";
    public static final String TABLE_NAME1 = "table1";
    public static final String TABLE_NAME2 = "table2";

    // path to uploaded files
    public static final String AVRO_SOURCE_URI_PATH = "gs://apt-index-293821_cloudbuild/";

    // Pub/Sub topic id
    public static final String TOPIC_ID = "clsub";

    //Avro schema
    public static final String SCHEMA = "{\"type\":\"record\", " +
            "\"name\":\"Client\", " +
            "\"fields\":[" +
            "{\"name\": \"id\", \"type\": \"long\"}, " +
            "{\"name\": \"name\", \"type\": \"string\"}," +
            "{\"name\": \"phone\", \"type\": [\"string\",\"null\"]}," +
            "{\"name\": \"address\", \"type\": [\"string\",\"null\"]}" +
            "]}";
}
